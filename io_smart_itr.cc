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

#include "circ.h"
#include "dbprintf.h"

int debug = 0;

template <typename T>
class io_smart_mmap
{
protected:
	int cache_capacity;
	/* How many elements to keep in the cache
	 * across iterations.
	 */
	int cache_overlap;

	int n_elms;
	circ_buf_t q;
	circ_buf_t io_q;
	int fd = -1;
	int head_idx = 0;
	int tail_idx = 0;
	int head_offset = 0;

public:
	int read_cnt = 0;

	io_smart_mmap(const char *path, size_t cache_capacity, size_t cache_overlap = 0)
		: cache_capacity(cache_capacity),
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
			if (pread(fd, &c, 1 * sizeof(T), tail_idx * sizeof(T)) < 0) {
				throw std::system_error(errno, std::system_category());
			}
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

	io_smart_mmap<uint16_t> m = io_smart_mmap<uint16_t>(argv[1], 4);

	for (int j=0; j<5; j++) {
		printf("Iteration %d\n", j);

		for (io_smart_mmap<uint16_t>::iterator it = m.begin();
		     it != m.end();
		     it++)
		{
			std::cout << *it << ' ';
		}

		std::cout << std::endl;

		m.printq();
		printf("\n");
	}

	printf("read_cnt = %d\n", m.read_cnt);

	return 0;
}
