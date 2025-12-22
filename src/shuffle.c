#include "mapreduce.h"
#include <stdlib.h>
#include <string.h>
#include "utils/hash_table.h"

// Shuffle阶段：将中间结果排序和分区
void shuffle(struct MapReduceContext *context) {
    // 在每个分区内，对键值对进行排序（按key排序）
    for (int i = 0; i < context->config.num_reducers; i++) {
        // 将链表转换为数组进行排序
        int count = 0;
        struct KeyValue *current = context->intermediate[i];
        
        while (current) {
            count++;
            current = current->next;
        }
        
        if (count == 0) continue;
        
        // 创建数组
        struct KeyValue **array = malloc(count * sizeof(struct KeyValue*));
        current = context->intermediate[i];
        for (int j = 0; j < count; j++) {
            array[j] = current;
            current = current->next;
        }
        
        // 按key排序（使用快速排序）
        qsort(array, count, sizeof(struct KeyValue*), compare_keys);
        
        // 重新构建链表
        context->intermediate[i] = array[0];
        for (int j = 0; j < count - 1; j++) {
            array[j]->next = array[j + 1];
        }
        array[count - 1]->next = NULL;
        
        free(array);
    }
}

// 比较函数：按key排序
int compare_keys(const void *a, const void *b) {
    struct KeyValue *kv1 = *(struct KeyValue**)a;
    struct KeyValue *kv2 = *(struct KeyValue**)b;
    
    return strcmp(kv1->key, kv2->key);
}