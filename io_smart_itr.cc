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
	pthread_mutex_t *resp_q_lock;
	circ_buf_t *resp_q;
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

#if 0
		fprintf(stdout, "Request %d fd %d pread size %zu from %zu\n",
			cnt,
			req.fd,
			req.offset,
			req.size);

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

		pthread_cond_signal(req.resp_q_cond);
		pthread_mutex_unlock(req.resp_q_lock);
	}

	free(buf);

	fprintf(stdout, "io thread exit\n");
	pthread_exit(NULL);
}


template <typename T>
class io_smart_mmap
{
protected:
	int cache_capacity;
	circ_buf_t *io_q;
	pthread_mutex_t *io_q_lock;
	pthread_cond_t *io_q_cond;
	/* How many elements to keep in the cache
	 * across iterations.
	 */
	int cache_overlap;

	int n_elms;
	circ_buf_t q;
	pthread_cond_t q_cond;
	pthread_mutex_t q_lock;

	int fd = -1;
	int head_idx = 0;
	int tail_idx = 0;
	int head_offset = 0;

public:
	int read_cnt = 0;

	io_smart_mmap(const char *path, size_t cache_capacity,
		      circ_buf_t *io_q,
		      pthread_mutex_t *io_q_lock,
		      pthread_cond_t *io_q_cond,
		      size_t cache_overlap = 0)
		: cache_capacity(cache_capacity),
		  io_q(io_q),
		  io_q_lock(io_q_lock),
		  io_q_cond(io_q_cond),
		  cache_overlap(cache_capacity - cache_overlap)
		{
			fd = open(path, O_RDONLY);
			if (fd < 0) {
				throw std::system_error(errno, std::system_category());
			}

			if (circ_init(&q, cache_capacity, sizeof(T)) < 0) {
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

			n_elms = sbuf.st_size / sizeof(T);

			prefill();
		}
	~io_smart_mmap()
		{
			circ_free(&q);
		}
	class iterator
	{
	public:
		iterator (int cnt, class io_smart_mmap *m)
			: cnt(cnt),
			  m(m)
			{}
		iterator &operator++() /* preincrement */
			{
				abort();
			}
		const iterator operator++(int) /* postincrement */
			{
				if (cnt < m->n_elms - m->cache_overlap) {
					T c;

					m->wait_on_q();

					pthread_mutex_lock(&m->q_lock);

					if (circ_deq(&m->q, &c) < 0) {
						printf("bogus!\n");
						abort();
					}

					pthread_mutex_unlock(&m->q_lock);

					//dbprintf("dequed %c\n", &c[0]);
					m->head_idx = (m->head_idx + 1) % m->n_elms;
					dbprintf("head_idx = %d\n", m->head_idx);
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
		T operator *()
			{
				m->wait_on_q(m->head_offset + 1);

				pthread_mutex_lock(&m->q_lock);

				T *p_c = (T *) circ_peek(&m->q, m->head_offset);

				dbprintf("read elm %2d, c = %c, cnt = %d\n", (m->head_idx + m->head_offset), *p_c, m->q.count);
				T c = *p_c;
				pthread_mutex_unlock(&m->q_lock);

				return c;
			}
		inline int idx()
			{
				return m->head_idx + m->head_offset;
			}
	private:
		int cnt;
		class io_smart_mmap *m;
	};
	void prefill()
		{
			for (int i=0; i<cache_capacity; i++) {
				fill_next();
			}
		}
	void wait_on_q(int count = 1)
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
			req.size = 1 * sizeof(T);
			req.offset = tail_idx * sizeof(T);
			req.exit_flag = 0;
			req.resp_q = &q;
			req.resp_q_lock = &q_lock;
			req.resp_q_cond = &q_cond;

			pthread_mutex_lock(io_q_lock);

			if (circ_enq(io_q, &req)) {
				pthread_mutex_unlock(io_q_lock);
				throw std::system_error(EBUSY, std::system_category());
			}

			pthread_cond_signal(io_q_cond);
			pthread_mutex_unlock(io_q_lock);

			read_cnt++;
			tail_idx = (tail_idx + 1) % n_elms;
		}
	iterator begin()
		{
			head_offset = 0;
			return iterator(0, this);
		}
	iterator end()
		{
			return iterator(n_elms, this);
		}
	void printq()
		{
			for (size_t i=0; i<q.count; i++) {
				T *p_c = (T *) circ_peek(&q, i);
				dbprintf("peek %2lu %c\n", i, *p_c);
			}
		}
};


pthread_t thread;
pthread_cond_t req_condition;
pthread_mutex_t req_queue_lock;
circ_buf_t req_queue;

int io_init(const char *path)
{
	if (pthread_cond_init(&req_condition, NULL)) {
		perror("pthread_cond_init");
		return -1;
	}

	if (pthread_mutex_init(&req_queue_lock, NULL)) {
		perror("pthread_mutex_init");
		return -1;
	}

	circ_init(&req_queue, 100, sizeof(struct pread_req));

	struct io_thread_args args = {
		.req_cond = &req_condition,
		.req_queue_lock = &req_queue_lock,
		.req_queue = &req_queue,
	};

	if (pthread_create(&thread, NULL, io_thread, &args) < 0) {
		perror("pthread_create");
		return -1;
	}

	return 0;
}

int io_term()
{
	struct pread_req exit_req;
	exit_req.exit_flag = 1;

	pthread_mutex_lock(&req_queue_lock);

	while (circ_enq(&req_queue, &exit_req) < 0) {
		sleep(1);
	}
	pthread_cond_signal(&req_condition);
	pthread_mutex_unlock(&req_queue_lock);

	pthread_join(thread, NULL);

	circ_free(&req_queue);
	return 0;
}


int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "Usage: prog filename\n");
		return 1;
	}

	io_init(argv[1]);

	io_smart_mmap<char> m = io_smart_mmap<char>(argv[1], 4,
						    &req_queue, &req_queue_lock, &req_condition, 4);

	for (int j=0; j<7; j++) {
		printf("Iteration %d\n", j);

		for (io_smart_mmap<char>::iterator it = m.begin();
		     it != m.end();
		     it++)
		{
			char c = *it;
			int idx = it.idx();
			printf("%c (%2d)  ", c, idx);
		}

		printf("\n");
	}

	printf("read_cnt = %d\n", m.read_cnt);

	io_term();

	return 0;
}
