#include "sort.h"
#include <stdlib.h>
#include <string.h>

// 快速排序分区函数
static int partition(void *array, int low, int high, size_t size,
                     int (*compare)(const void*, const void*)) {
    char *arr = (char*)array;
    void *pivot = malloc(size);
    memcpy(pivot, &arr[high * size], size);
    
    int i = low - 1;
    
    for (int j = low; j < high; j++) {
        if (compare(&arr[j * size], pivot) < 0) {
            i++;
            // 交换元素
            for (size_t k = 0; k < size; k++) {
                char temp = arr[i * size + k];
                arr[i * size + k] = arr[j * size + k];
                arr[j * size + k] = temp;
            }
        }
    }
    
    i++;
    // 交换枢轴
    for (size_t k = 0; k < size; k++) {
        char temp = arr[i * size + k];
        arr[i * size + k] = arr[high * size + k];
        arr[high * size + k] = temp;
    }
    
    free(pivot);
    return i;
}

// 快速排序递归实现
static void quick_sort(void *array, int low, int high, size_t size,
                       int (*compare)(const void*, const void*)) {
    if (low < high) {
        int pi = partition(array, low, high, size, compare);
        quick_sort(array, low, pi - 1, size, compare);
        quick_sort(array, pi + 1, high, size, compare);
    }
}

// 快速排序包装函数
void sort(void *array, int count, size_t size,
          int (*compare)(const void*, const void*)) {
    quick_sort(array, 0, count - 1, size, compare);
}