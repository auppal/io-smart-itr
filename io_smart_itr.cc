/* IO-aware memory-mapped array iterator.
 * Copyright (C) 2015 Ahsen Uppal
 * This program can be distributed under the terms
 * of the GNU GENERAL PUBLIC LICENSE, Version 3.
 * See the file LICENSE.
 */

/* For an example of a simple c++ iterator, see:
 * https://gist.github.com/jeetsukumaran/307264
 */

#include <system_error>
#include <iterator>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h> /* pread */
#include <semaphore.h>
#include <pthread.h>
#include <assert.h>
#include <string.h> /* memcpy */

#include "circ.h"
#include "dbprintf.h"

int debug = 0;

struct io_thread_args {
	pthread_cond_t *req_cond;
	pthread_mutex_t *req_queue_lock;
	circ_buf_t *req_queue;
};

struct pread_req {
	int fd;
	size_t size;
	off_t offset;
	size_t idx;
	pthread_mutex_t *resp_q_lock;
	circ_buf_t *resp_q;
	circ_buf_t *resp_q_idx;
	pthread_cond_t *resp_q_cond;
	int exit_flag;
};

static void *io_thread(void *arg)
{
	struct io_thread_args *args = (struct io_thread_args *) arg;

	pthread_cond_t *req_cond = args->req_cond;
	pthread_mutex_t *req_queue_lock = args->req_queue_lock;
	circ_buf_t *queue = args->req_queue;

	struct pread_req req;
	int cnt = 0;

	static void *buf = 0;
	static size_t buf_size = 0;

	for (;;) {
		pthread_mutex_lock(req_queue_lock);

		while (circ_cnt(queue) == 0) {
			pthread_cond_wait(req_cond, req_queue_lock);
		}

		cnt++;
		circ_deq(queue, &req);
		/* ignore empty queue which should never occur here */
		pthread_mutex_unlock(req_queue_lock);

		if (req.exit_flag) {
			fprintf(stderr, "Exit flag set - breaking.\n");
			break;
		}

		if (req.size == 0) {
			/* Empty request for synchronization only. */
			pthread_cond_signal(req.resp_q_cond);
			pthread_mutex_unlock(req.resp_q_lock);
			continue;
		}

#if 0
		fprintf(stdout, "Request %d fd %d pread size %zu from %zu idx %zu\n",
			cnt,
			req.fd,
			req.offset,
			req.size,
			req.idx);

		fflush(stdout);
#endif
		if (req.size > buf_size) {
			buf = realloc(buf, req.size);
		}

		if (!buf) {
			perror("realloc");
			abort();
		}

		if (pread(req.fd, buf, req.size, req.offset) < 0) {
			perror("pread");
			abort();
		}

		pthread_mutex_lock(req.resp_q_lock);

		if (circ_enq(req.resp_q, buf) < 0) {
			perror("circ_enq");
			abort();
		}

		if (circ_enq(req.resp_q_idx, &req.idx) < 0) {
			perror("circ_enq");
			abort();
		}

		pthread_cond_signal(req.resp_q_cond);
		pthread_mutex_unlock(req.resp_q_lock);
	}

	free(buf);

	fprintf(stdout, "io thread exit\n");
	pthread_exit(NULL);
}


//template <typename T>
class io_smart_mmap
{
protected:
	int cache_capacity;
	size_t object_size;
	/* How many elements to keep in the cache
	 * across iterations.
	 */
	size_t cache_overlap;

	size_t n_elms;
	circ_buf_t q;
	circ_buf_t q_idx;
	pthread_cond_t q_cond;
	pthread_mutex_t q_lock;

	int fd = -1;
	int head_idx = 0;
	int tail_idx = 0;
	int head_offset = 0;


	pthread_t thread;
	pthread_cond_t io_queue_cond;
	pthread_mutex_t io_queue_lock;
	circ_buf_t io_queue;

	struct io_thread_args args;

	uint8_t *filter_bits;
	size_t filter_bits_size;
	ssize_t filter_bits_highest = -1;
	size_t n_filtered_elms;
public:
	int read_cnt = 0;
	size_t get_n_elms()
		{
			return n_elms;
		}
	size_t get_filtered_n_elms()
		{
			return n_filtered_elms;
		}
	io_smart_mmap(const char *path,
		      size_t cache_capacity,
		      size_t object_size,
		      size_t cache_overlap = 0)
		: cache_capacity(cache_capacity),
		  object_size(object_size),
		  cache_overlap(cache_capacity - cache_overlap),
		  filter_bits(0)
		{
			if (io_thread_init() < 0) {
				throw std::system_error(errno, std::system_category());
			}

			fd = open(path, O_RDONLY);
			if (fd < 0) {
				throw std::system_error(errno, std::system_category());
			}

			if (circ_init(&q, cache_capacity, object_size) < 0) {
				throw std::system_error(errno, std::system_category());
			}

			if (circ_init(&q_idx, cache_capacity, sizeof(size_t)) < 0) {
				throw std::system_error(errno, std::system_category());
			}

			if (pthread_cond_init(&q_cond, NULL)) {
				throw std::system_error(errno, std::system_category());
			}
			if (pthread_mutex_init(&q_lock, NULL)) {
				throw std::system_error(errno, std::system_category());
			}

			struct stat sbuf;

			if (fstat(fd, &sbuf) < 0) {
				throw std::system_error(errno, std::system_category());
			}

			n_elms = sbuf.st_size / object_size;
			n_filtered_elms = n_elms;

			prefill();
		}
	~io_smart_mmap()
		{
			io_thread_term();
			circ_free(&q);
			circ_free(&q_idx);
			free(filter_bits);
		}
	class iterator
	{
	public:
		iterator (size_t cnt, class io_smart_mmap *m)
			: cnt(cnt),
			  m(m)
			{}
		iterator &operator++() /* preincrement */
			{
				abort();
			}
		const iterator next()
			{
				return this->operator++(0);
			}
		const iterator operator++(int) /* postincrement */
			{
				if (cnt + m->cache_overlap < m->n_filtered_elms) {
					m->wait_on_q();

					/* Discard the current head
					 * and fill the next element
					 * from the tail.
					 */

					pthread_mutex_lock(&m->q_lock);

					if (circ_deq(&m->q, NULL) < 0) {
						abort();
					}

					if (circ_deq(&m->q_idx, NULL) < 0) {
						abort();
					}

					pthread_mutex_unlock(&m->q_lock);

					//dbprintf("dequed %c\n", &c[0]);
					m->head_idx = m->next_idx(m->head_idx);
					//dbprintf("head_idx = %d\n", m->head_idx);
					m->fill_next();
				}
				else {
					m->head_offset++;
				}

				iterator it(cnt, m);
				cnt++;
				return it;
			}
		bool operator==(const iterator &it)
			{
				return (cnt == it.cnt);
			}
		bool operator!=(const iterator &it)
			{
				return (cnt != it.cnt);
			}
		void* operator *()
			{
				//printf("head_offset = %d\n", m->head_offset);
				m->wait_on_q(m->head_offset + 1);

				pthread_mutex_lock(&m->q_lock);

				void *p_obj = (void *) circ_peek(&m->q, m->head_offset);

				//dbprintf("read elm %2d, c = %c, cnt = %d\n", (m->head_idx + m->head_offset), *p_c, m->q.count);
				pthread_mutex_unlock(&m->q_lock);

				return p_obj;
			}
		inline int idx()
			{
				pthread_mutex_lock(&m->q_lock);
				size_t *p_idx = (size_t *) circ_peek(&m->q_idx, m->head_offset);
				size_t idx = *p_idx;
				pthread_mutex_unlock(&m->q_lock);
				return idx;
				//return (m->head_idx + m->head_offset) % m->n_elms;
			}
	private:
		size_t cnt;
		class io_smart_mmap *m;
	};
	void prefill()
		{
			pthread_mutex_lock(&q_lock);
			int N = cache_capacity - circ_cnt(&q);
			pthread_mutex_unlock(&q_lock);

			for (int i=0; i<N; i++) {
				fill_next();
			}
		}
	void wait_on_q(size_t count = 1)
		{
			pthread_mutex_lock(&q_lock);
			while (circ_cnt(&q) < count) {
				pthread_cond_wait(&q_cond, &q_lock);
			}
			assert(circ_cnt(&q) >= count);
			pthread_mutex_unlock(&q_lock);
		}
	void fill_next()
		{
			struct pread_req req;
			req.fd = fd;
			req.size = 1 * object_size;

			dbg("fill_next: request idx %d\n", tail_idx);

			req.idx = tail_idx;
			req.offset = tail_idx * object_size;
			req.exit_flag = 0;
			req.resp_q = &q;
			req.resp_q_idx = &q_idx;
			req.resp_q_lock = &q_lock;
			req.resp_q_cond = &q_cond;

			pthread_mutex_lock(&io_queue_lock);

			if (circ_enq(&io_queue, &req)) {
				pthread_mutex_unlock(&io_queue_lock);
				throw std::system_error(EBUSY, std::system_category());
			}

			pthread_cond_signal(&io_queue_cond);
			pthread_mutex_unlock(&io_queue_lock);

			read_cnt++;
			tail_idx = next_idx(tail_idx);
		}
	size_t next_idx(size_t idx)
		{
			if (filter_bits) {
				size_t i = ((idx + 1) / 8) % filter_bits_size;
				size_t j = (idx + 1) % 8;

				for (;;) {
					while (j <  8) {
						if (filter_bits[i] & (1 << j))
							goto done;
						j++;
					}

					j = 0;
					if (i++ == filter_bits_size)
						i = 0;
				}
done:
				return (i * 8 + j) % n_elms;
			}
			else {
				return (idx + 1) % n_elms;
			}
		}
	iterator begin()
		{
			head_offset = 0;
			return iterator(0, this);
		}
	iterator end()
		{
			return iterator(n_filtered_elms, this);
		}
	void printq()
		{
			pthread_mutex_lock(&q_lock);
	
			printf("[ ");
			for (size_t i=0; i<circ_cnt(&q_idx); i++) {
				size_t *p_idx = (size_t *) circ_peek(&q_idx, i);
				size_t idx = *p_idx;
				printf(" %lu ", idx);
			}
			printf("]\n");
			pthread_mutex_unlock(&q_lock);
		}
	/* Set 1-bit ignore filter for each page in this
	 * mapping.
	 */
	int set_filter(const uint8_t *filter_bits,
			size_t filter_bits_size_bytes)
		{
			void *p = realloc(this->filter_bits, filter_bits_size_bytes);

			if (!p) {
				throw std::system_error(errno, std::system_category());
			}

			ssize_t highest_bit = -1;
			size_t cnt = 0;
			size_t i, j;
			ssize_t tail_idx_new = -1;

			for (i=0; i<filter_bits_size_bytes; i++) {
				for (j=0; j<8; j++) {

					if (8 * i + j >= n_elms)
						break;

					if (filter_bits[i] & (1 << j)) {
						cnt++;
						highest_bit = 8 * i + j;
						if (tail_idx_new == -1)
							tail_idx_new = 8 * i + j;
					}
				}
			}
			tail_idx = tail_idx_new;

			if (cnt < 2) {
				return -1;
			}

			this->filter_bits = (uint8_t *) p;
			memcpy(this->filter_bits, filter_bits, filter_bits_size_bytes);

			filter_bits_highest = highest_bit;
			n_filtered_elms = cnt;
			filter_bits_size = filter_bits_size_bytes;


			/* For now, drop the cached elements
			 * entirely.
			 */
#if 1
			pthread_mutex_lock(&q_lock);
			circ_clear(&q);
			circ_clear(&q_idx);
			pthread_mutex_unlock(&q_lock);
			prefill();
#else
			/* Wait for any previous I/O ops to drain */
			pthread_cond_t drain_cond;
			pthread_mutex_t drain_mutex;;
			struct pread_req req;

			req.exit_flag = 0;
			req.size = 0;
			req.resp_q_lock = &drain_mutex;
			req.resp_q_cond = &drain_cond;;

			if (pthread_cond_init(&drain_cond, NULL)) {
				throw std::system_error(EBUSY, std::system_category());
			}

			if (pthread_mutex_init(&drain_mutex, NULL)) {
				throw std::system_error(errno, std::system_category());
			}

			pthread_mutex_lock(&io_queue_lock);

			if (circ_enq(&io_queue, &req)) {
				pthread_mutex_unlock(&io_queue_lock);
				throw std::system_error(EBUSY, std::system_category());
			}

			pthread_cond_signal(&io_queue_cond);
			pthread_mutex_unlock(&io_queue_lock);

			pthread_mutex_lock(&drain_mutex);
			pthread_cond_wait(&drain_cond, &drain_mutex);

			/* Filter all previously cached elements */

			//dbg("circ_cnt_req = %d\n", circ_cnt(&io_queue));

			tail_idx_new = -1;
			pthread_mutex_lock(&q_lock);

			dbg("circ_cnt_results = %d\n", circ_cnt(&q));

			size_t idx;

			for (idx=0; idx<circ_cnt(&q_idx); idx++) {
				size_t *p_idx = (size_t *) circ_peek(&q_idx, idx);
				i = *p_idx / 8;
				j = *p_idx % 8;

				if ((filter_bits[i] & (1 << j)) == 0) {
					dbg("Evict %lu\n", *p_idx);
					circ_del(&q_idx, idx);
					circ_del(&q, idx);
					idx--;
				}
				else {
					dbg("Keep %lu\n", *p_idx);
					tail_idx_new = *p_idx;
				}
			}

			pthread_mutex_unlock(&q_lock);

			if (tail_idx_new != -1)
				tail_idx = next_idx(tail_idx_new);


			/* Now, prefill the cache rest of the cache. */
			prefill();

			dbg("n_filtered_elms = %d\n", n_filtered_elms);
			dbg("filter_bits_highest = %d\n", filter_bits_highest);
			dbg("tail_idx = %d\n", tail_idx);
#endif

			return 0;
		}
private:
	int io_thread_init()
	{
		if (pthread_cond_init(&io_queue_cond, NULL)) {
			return -1;
		}

		if (pthread_mutex_init(&io_queue_lock, NULL)) {
			return -1;
		}

		circ_init(&io_queue, 100, sizeof(struct pread_req));

		args.req_cond = &io_queue_cond;
		args.req_queue_lock = &io_queue_lock;
		args.req_queue = &io_queue;

		if (pthread_create(&thread, NULL, io_thread, &args) < 0) {
			return -1;
		}

		return 0;
	}

	int io_thread_term()
	{
		struct pread_req exit_req;
		exit_req.exit_flag = 1;

		pthread_mutex_lock(&io_queue_lock);

		while (circ_enq(&io_queue, &exit_req) < 0) {
			sleep(1);
		}
		pthread_cond_signal(&io_queue_cond);
		pthread_mutex_unlock(&io_queue_lock);

		pthread_join(thread, NULL);

		circ_free(&io_queue);
		return 0;
	}
};

int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "Usage: prog filename\n");
		return 1;
	}

	io_smart_mmap m = io_smart_mmap(argv[1], 4, sizeof(char), 0);

	for (int j=0; j<7; j++) {
		printf("Iteration %d\n", j);

		for (io_smart_mmap::iterator it = m.begin();
		     it != m.end();
		     it++)
		{
			char *p_c = (char *) *it;
			int idx = it.idx();
			printf("%c (%2d) ", *p_c, idx);
		}

		printf("\n");
	}

	printf("\n");

	uint8_t all_filter[] =  { 0xff, 0xff };
	uint8_t odd_filter[] =  { 0xaa, 0xaa };
	uint8_t even_filter[] = { 0x55, 0x55 };
	uint8_t *filters[] = {all_filter, even_filter, all_filter, odd_filter};
	uint8_t *f;

	unsigned i;
	for (i=0; i<sizeof(filters)/sizeof(filters[0]); i++) {
		f = filters[i];

		printf("set_filter\n");
		m.printq();
		m.set_filter(f, sizeof(even_filter));
		m.printq();

		for (io_smart_mmap::iterator it = m.begin();
		     it != m.end();
		     it++)
		{
			char *p_c = (char *) *it;
			int idx = it.idx();
			printf("%c (%2d)  \n", *p_c, idx);
		}

		printf("\n\n");
	}

	printf("read_cnt = %d\n", m.read_cnt);

	return 0;
}
