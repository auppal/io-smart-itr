/* Simple debug printf libray.
 * Copyright (C) 2015 Ahsen Uppal
 * This program can be distributed under the terms of the GNU GPL.
 * See the file LICENSE.
 */

#include "dbprintf.h"
#include <stdio.h>

int dbprintf(const char *fmt, ...)
{
	int rc = 0;

	va_list ap;
	va_start(ap, fmt);

	if (debug) {
		rc = vprintf(fmt, ap);
	}

	va_end(ap);
	return rc;
}
