#ifndef SORT_H
#define SORT_H

#include <stddef.h>

// 排序函数声明
void sort(void *array, int count, size_t size,
          int (*compare)(const void*, const void*));

#endif