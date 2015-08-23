/* Simple circular buffer.
 * Copyright (C) 2015 Ahsen Uppal
 * This program can be distributed under the terms
 * of the GNU GENERAL PUBLIC LICENSE, Version 3.
 * See the file LICENSE.
 */

#include "circ.h"
#include <stdlib.h>
#include <string.h>

int circ_init(circ_buf_t *b, unsigned int len, unsigned int size)
{
	b->buf = malloc((len + 1) * size);

	if (!b->buf) {
		return -1;
	}

	b->len = (len + 1);
	b->size = size;
	b->tail = 0;
	b->head = 0;
	b->count = 0;

	return 0;
}

int circ_enq(circ_buf_t *b, const void *elm)
{
	int tail = (b->tail + 1) % b->len;

	if (tail == b->head) {
		return -1;
	}

	memcpy(b->buf + b->tail * b->size, elm, b->size);
	b->tail = tail;
	b->count++;
	return 0;
}

int circ_deq(circ_buf_t *b, void *elm)
{
	if (b->tail == b->head) {
		return -1;
	}

	if (elm) {
		memcpy(elm, &b->buf[b->head * b->size], b->size);
	}

	b->head = (b->head + 1) % b->len;
	b->count--;
	return 0;
}

const void *circ_peek(circ_buf_t *b, int index)
{
	if (index >= b->count)
		return NULL;

	int i = (b->head + index) % b->len;
	return &b->buf[i * b->size];
}

unsigned int circ_cnt(circ_buf_t *b)
{
	return b->count;
}

void circ_free(circ_buf_t *b)
{
	if (b) {
		free(b->buf);
	}
}
