#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "utils/hash_table.h"

// Reducer函数：对相同key的值求和
void wordcount_reducer(int partition, void *context_ptr) {
    struct MapReduceContext *context = (struct MapReduceContext*)context_ptr;
    
    // 使用哈希表聚合相同key的值
    struct HashTable *ht = hash_table_create(1000);
    
    struct KeyValue *current = context->intermediate[partition];
    while (current) {
        char *key = current->key;
        int count = *(int*)current->value;
        
        // 查找或创建
        int *existing = hash_table_get(ht, key);
        if (existing) {
            *existing += count;
        } else {
            int *new_count = malloc(sizeof(int));
            *new_count = count;
            hash_table_insert(ht, strdup(key), new_count);
        }
        
        current = current->next;
    }
    
    // 将结果发射到最终输出
    struct HashTableIterator it;
    hash_table_iter_init(&it, ht);
    
    while (hash_table_iter_next(&it)) {
        emit_final(context_ptr, it.key, it.value, sizeof(int));
    }
    
    // 清理哈希表（注意：不释放key和value，因为它们已被转移）
    hash_table_destroy_keep_data(ht);
}