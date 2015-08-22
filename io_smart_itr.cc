// See:
// https://gist.github.com/jeetsukumaran/307264

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
	void *buf;
	sem_t *sem;
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

		if (pread(req.fd, req.buf, req.size, req.offset) < 0) {
			perror("pread");
			abort();
		}

		if (req.sem) {
			sem_post(req.sem);
		}
	}

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

					if (circ_deq(&m->q, &c) < 0) {
						throw std::system_error(errno, std::system_category());
					}
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
				T *p_c = (T *) circ_peek(&m->q, m->head_offset);

				dbprintf("read elm %2d, c = %c, cnt = %d\n", (m->head_idx + m->head_offset), *p_c, m->q.count);
				return *p_c;
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

			dbprintf("Prefilled\n");
			printq();
		}
	void fill_next()
		{
			T c;
#if 0
			if (pread(fd, &c, 1 * sizeof(T), tail_idx * sizeof(T)) < 0) {
				throw std::system_error(errno, std::system_category());
			}
#else
			sem_t sem;
			sem_init(&sem, 0, 0);

			struct pread_req req;
			req.fd = fd;
			req.buf = &c;
			req.size = 1 * sizeof(T);
			req.offset = tail_idx * sizeof(T);
			req.sem = &sem;
			req.exit_flag = 0;

			pthread_mutex_lock(io_q_lock);

			if (circ_enq(io_q, &req)) {
				pthread_mutex_unlock(io_q_lock);
				throw std::system_error(EBUSY, std::system_category());
			}

			pthread_cond_signal(io_q_cond);
			pthread_mutex_unlock(io_q_lock);
			sem_wait(&sem);
#endif
			read_cnt++;
			circ_enq(&q, &c);
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

int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "Usage: prog filename\n");
		return 1;
	}

	pthread_cond_t req_condition;
	pthread_mutex_t req_queue_lock;
	circ_buf_t req_queue;

	if (pthread_cond_init(&req_condition, NULL)) {
		perror("pthread_cond_init");
	}
	if (pthread_mutex_init(&req_queue_lock, NULL)) {
		perror("pthread_mutex_init");
		return 1;
	}

	circ_init(&req_queue, 100, sizeof(struct pread_req));

	struct io_thread_args args = {
		.req_cond = &req_condition,
		.req_queue_lock = &req_queue_lock,
		.req_queue = &req_queue,
	};

	pthread_t thread;

	if (pthread_create(&thread, NULL, io_thread, &args) < 0) {
		fprintf(stderr, "error creating thread\n");
	}

	io_smart_mmap<char> m = io_smart_mmap<char>(argv[1], 4,
						&req_queue, &req_queue_lock, &req_condition);

	for (int j=0; j<5; j++) {
		printf("Iteration %d\n", j);

		for (io_smart_mmap<char>::iterator it = m.begin();
		     it != m.end();
		     it++)
		{
			char c = *it;
#if 1
			printf("%c ", c);
#else
			std::cout << c << ' ';
#endif
		}

		m.printq();
		printf("\n");
#if 0
		std::cout << std::endl;
#endif
	}

	printf("read_cnt = %d\n", m.read_cnt);

	struct pread_req exit_req;
	exit_req.exit_flag = 1,

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
