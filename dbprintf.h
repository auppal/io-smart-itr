/* Simple debug printf libray.
 * Copyright (C) 2015 Ahsen Uppal
 * This program can be distributed under the terms
 * of the GNU GENERAL PUBLIC LICENSE, Version 3.
 * See the file LICENSE.
 */

#include <stdarg.h>

#define dbg(fmt, ...) dbprintf("%s:%d " fmt, __FILE__, __LINE__, __VA_ARGS__)

#ifdef __cplusplus
extern "C" {
#endif
	int dbprintf(const char *fmt, ...);
	extern int debug;

#ifdef __cplusplus
}
#endif
