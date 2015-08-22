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

class io_smart_mmap
{
protected:
	int n_elms, cache_capacity;
	circ_buf_t q;
	circ_buf_t io_q;
	int fd = -1;
	int head_idx = 0;
	int tail_idx = 0;

public:
	io_smart_mmap(const char *path, size_t cache_capacity)
		: cache_capacity(cache_capacity)
		{
			fd = open(path, O_RDONLY);
			if (fd < 0) {
				throw std::system_error(errno, std::system_category());
			}

			if (circ_init(&q, cache_capacity, sizeof(char)) < 0) {
				throw std::system_error(errno, std::system_category());
			}

			struct stat sbuf;

			if (fstat(fd, &sbuf) < 0) {
				throw std::system_error(errno, std::system_category());
			}

			n_elms = sbuf.st_size;

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
				char c;

				if (circ_deq(&m->q, &c) < 0) {
					throw std::system_error(errno, std::system_category());
				}
				m->head_idx = (m->head_idx + 1) % m->n_elms;

				printf("dequed %c, head_idx = %d\n", c, m->head_idx);

				m->fill_next();

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
		char operator *()
			{
				char *p_c = (char *) circ_peek(&m->q, 0);

				printf("read elm %2d, c = %c, cnt = %d\n", m->head_idx, *p_c, m->q.count);
				return *p_c;
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

			printf("Prefilled\n");
			printq();
		}
	void fill_next()
		{
			char c;
			if (pread(fd, &c, 1 * sizeof(char), tail_idx * sizeof(char)) < 0) {
				throw std::system_error(errno, std::system_category());
			}
			circ_enq(&q, &c);
			tail_idx = (tail_idx + 1) % n_elms;
		}
	iterator begin()
		{
			return iterator(0, this);
		}
	iterator end()
		{
			return iterator(n_elms, this);
		}
	void printq()
		{
			for (size_t i=0; i<q.count; i++) {
				char *p_c = (char *) circ_peek(&q, i);
				printf("peek %2lu %c\n", i, *p_c);
			}
		}
};

int main(int argc, char *argv[])
{

	if (argc < 2) {
		fprintf(stderr, "Usage: prog filename\n");
		return 1;
	}

	io_smart_mmap m = io_smart_mmap(argv[1], 4);

	for (int j=0; j<5; j++) {
		printf("Iteration %d\n", j);

		for (io_smart_mmap::iterator it = m.begin();
		     it != m.end();
		     it++)
		{
			//std::cout << *it << std::endl;
			*it;
		}

		m.printq();
		printf("\n");
	}

	return 0;
}