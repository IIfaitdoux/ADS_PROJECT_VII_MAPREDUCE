#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>

// 前向声明
struct HashTable;
struct HashTableIterator;

// MapReduce配置
struct MapReduceConfig {
    int num_mappers;
    int num_reducers;
    char **input_files;
    int num_input_files;
    char *output_file;
};

// 键值对结构
struct KeyValue {
    char *key;
    void *value;
    size_t value_size;
    struct KeyValue *next;
};

// MapReduce上下文
struct MapReduceContext {
    struct MapReduceConfig config;
    struct KeyValue **intermediate;  // 中间结果
    struct KeyValue **final_results; // 最终结果
    pthread_mutex_t *mutexes;
    pthread_t *mapper_threads;
    pthread_t *reducer_threads;
};

// 哈希表相关函数声明
struct HashTable* hash_table_create(int size);
void hash_table_insert(struct HashTable *ht, char *key, void *value);
void* hash_table_get(struct HashTable *ht, const char *key);
void hash_table_iter_init(struct HashTableIterator *it, struct HashTable *ht);
int hash_table_iter_next(struct HashTableIterator *it);
void hash_table_destroy_keep_data(struct HashTable *ht);

// MapReduce函数声明
struct MapReduceContext* mapreduce_init(struct MapReduceConfig config);
int mapreduce_run(struct MapReduceContext *context);
void mapreduce_cleanup(struct MapReduceContext *context);
void emit_intermediate(void *context_ptr, char *key, void *value, size_t size);
void emit_final(void *context_ptr, char *key, void *value, size_t size);
void shuffle(struct MapReduceContext *context);
int compare_keys(const void *a, const void *b);
int compare_word_count(const void *a, const void *b);
void write_final_results(struct MapReduceContext *context);

#endif