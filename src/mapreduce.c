#include "mapreduce.h"
#include "utils/hash_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>

// 工作线程参数结构
typedef struct {
    int id;
    struct MapReduceContext *context;
    void **data;
    int data_count;
} ThreadArg;

// 哈希函数：将key映射到reducer分区
int hash_key(const char *key, int num_reducers) {
    unsigned long hash = 5381;
    int c;
    
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    
    return hash % num_reducers;
}

// 初始化MapReduce上下文
struct MapReduceContext* mapreduce_init(struct MapReduceConfig config) {
    struct MapReduceContext *context = malloc(sizeof(struct MapReduceContext));
    if (!context) return NULL;
    
    context->config = config;
    
    // 分配中间结果和最终结果数组
    context->intermediate = calloc(config.num_reducers, sizeof(struct KeyValue*));
    context->final_results = calloc(config.num_reducers, sizeof(struct KeyValue*));
    
    // 初始化互斥锁（每个分区一个）
    context->mutexes = malloc(config.num_reducers * sizeof(pthread_mutex_t));
    for (int i = 0; i < config.num_reducers; i++) {
        pthread_mutex_init(&context->mutexes[i], NULL);
    }
    
    context->mapper_threads = malloc(config.num_mappers * sizeof(pthread_t));
    context->reducer_threads = malloc(config.num_reducers * sizeof(pthread_t));
    
    return context;
}

// 发射中间结果
void emit_intermediate(void *context_ptr, char *key, void *value, size_t size) {
    struct MapReduceContext *context = (struct MapReduceContext*)context_ptr;
    
    // 计算分区
    int partition = hash_key(key, context->config.num_reducers);
    
    // 创建新的键值对
    struct KeyValue *kv = malloc(sizeof(struct KeyValue));
    kv->key = strdup(key);
    kv->value = malloc(size);
    memcpy(kv->value, value, size);
    kv->value_size = size;
    kv->next = NULL;
    
    // 加锁保护分区链表
    pthread_mutex_lock(&context->mutexes[partition]);
    
    if (context->intermediate[partition] == NULL) {
        context->intermediate[partition] = kv;
    } else {
        // 添加到链表末尾
        struct KeyValue *current = context->intermediate[partition];
        while (current->next) {
            current = current->next;
        }
        current->next = kv;
    }
    
    pthread_mutex_unlock(&context->mutexes[partition]);
}

// 发射最终结果
void emit_final(void *context_ptr, char *key, void *value, size_t size) {
    struct MapReduceContext *context = (struct MapReduceContext*)context_ptr;
    
    int partition = hash_key(key, context->config.num_reducers);
    
    struct KeyValue *kv = malloc(sizeof(struct KeyValue));
    kv->key = strdup(key);
    kv->value = malloc(size);
    memcpy(kv->value, value, size);
    kv->value_size = size;
    kv->next = NULL;
    
    pthread_mutex_lock(&context->mutexes[partition]);
    
    if (context->final_results[partition] == NULL) {
        context->final_results[partition] = kv;
    } else {
        struct KeyValue *current = context->final_results[partition];
        while (current->next) {
            current = current->next;
        }
        current->next = kv;
    }
    
    pthread_mutex_unlock(&context->mutexes[partition]);
}

// Mapper线程函数
void* mapper_thread_func(void *arg) {
    ThreadArg *thread_arg = (ThreadArg*)arg;
    struct MapReduceContext *context = thread_arg->context;
    
    // 处理分配的文件
    for (int i = 0; i < thread_arg->data_count; i++) {
        char *filename = (char*)thread_arg->data[i];
        
        // 打开并处理文件
        FILE *file = fopen(filename, "r");
        if (!file) {
            fprintf(stderr, "Mapper %d: Error opening file %s\n", thread_arg->id, filename);
            continue;
        }
        
        char word[256];
        while (fscanf(file, "%255s", word) == 1) {
            // 清理单词：转换为小写，移除标点
            int len = strlen(word);
            int j = 0;
            for (int k = 0; k < len; k++) {
                if (isalpha(word[k])) {
                    word[j++] = tolower(word[k]);
                }
            }
            word[j] = '\0';
            
            if (strlen(word) > 0) {
                int *value = malloc(sizeof(int));
                *value = 1;
                emit_intermediate(context, word, value, sizeof(int));
                free(value);
            }
        }
        
        fclose(file);
    }
    
    free(thread_arg->data);
    free(thread_arg);
    return NULL;
}

// Reducer线程函数
void* reducer_thread_func(void *arg) {
    ThreadArg *thread_arg = (ThreadArg*)arg;
    struct MapReduceContext *context = thread_arg->context;
    
    // 获取该reducer的中间结果分区
    int partition = thread_arg->id;
    struct KeyValue *current = context->intermediate[partition];
    
    // 使用哈希表聚合相同key的值
    struct HashTable *ht = hash_table_create(1000);
    
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
    
    // 将结果保存到最终输出
    struct HashTableIterator it;
    hash_table_iter_init(&it, ht);
    
    while (hash_table_iter_next(&it)) {
        emit_final(context, it.key, it.value, sizeof(int));
    }
    
    // 清理哈希表（注意：不释放key和value，因为它们已被转移）
    hash_table_destroy_keep_data(ht);
    
    free(thread_arg);
    return NULL;
}

// 执行Shuffle阶段（排序中间结果）
void shuffle(struct MapReduceContext *context) {
    // 在每个分区内，对键值对进行排序（按key排序）
    for (int i = 0; i < context->config.num_reducers; i++) {
        if (!context->intermediate[i]) continue;
        
        // 将链表转换为数组进行排序
        int count = 0;
        struct KeyValue *current = context->intermediate[i];
        while (current) {
            count++;
            current = current->next;
        }
        
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

// 比较函数：先按计数降序，再按单词字典序
int compare_word_count(const void *a, const void *b) {
    struct KeyValue *kv1 = *(struct KeyValue**)a;
    struct KeyValue *kv2 = *(struct KeyValue**)b;
    
    int count1 = *(int*)kv1->value;
    int count2 = *(int*)kv2->value;
    
    if (count1 != count2) {
        return count2 - count1; // 降序
    }
    return strcmp(kv1->key, kv2->key); // 字典序
}

// 合并最终结果并写入文件
void write_final_results(struct MapReduceContext *context) {
    FILE *output = fopen(context->config.output_file, "w");
    if (!output) {
        fprintf(stderr, "Error opening output file: %s\n", context->config.output_file);
        return;
    }
    
    // 收集所有最终结果
    int total_count = 0;
    for (int i = 0; i < context->config.num_reducers; i++) {
        struct KeyValue *current = context->final_results[i];
        while (current) {
            total_count++;
            current = current->next;
        }
    }
    
    // 创建数组
    struct KeyValue **all_results = malloc(total_count * sizeof(struct KeyValue*));
    int index = 0;
    for (int i = 0; i < context->config.num_reducers; i++) {
        struct KeyValue *current = context->final_results[i];
        while (current) {
            all_results[index++] = current;
            current = current->next;
        }
    }
    
    // 排序：先按计数降序，再按单词字典序
    qsort(all_results, total_count, sizeof(struct KeyValue*), compare_word_count);
    
    // 写入文件
    for (int i = 0; i < total_count; i++) {
        fprintf(output, "%s %d\n", all_results[i]->key, *(int*)all_results[i]->value);
    }
    
    fclose(output);
    free(all_results);
}

// 运行MapReduce作业
int mapreduce_run(struct MapReduceContext *context) {
    if (!context) return -1;
    
    // 1. Map阶段：创建mapper线程
    printf("Starting Map phase with %d mappers...\n", context->config.num_mappers);
    
    // 为每个mapper分配文件
    int files_per_mapper = context->config.num_input_files / context->config.num_mappers;
    int remainder = context->config.num_input_files % context->config.num_mappers;
    
    int current_file = 0;
    for (int i = 0; i < context->config.num_mappers; i++) {
        ThreadArg *arg = malloc(sizeof(ThreadArg));
        arg->id = i;
        arg->context = context;
        
        int files_for_this_mapper = files_per_mapper;
        if (i < remainder) {
            files_for_this_mapper++;
        }
        
        arg->data = malloc(files_for_this_mapper * sizeof(char*));
        arg->data_count = files_for_this_mapper;
        
        for (int j = 0; j < files_for_this_mapper; j++) {
            arg->data[j] = context->config.input_files[current_file++];
        }
        
        pthread_create(&context->mapper_threads[i], NULL, mapper_thread_func, arg);
    }
    
    // 等待所有mapper完成
    for (int i = 0; i < context->config.num_mappers; i++) {
        pthread_join(context->mapper_threads[i], NULL);
    }
    printf("Map phase completed.\n");
    
    // 2. Shuffle阶段：排序中间结果
    printf("Starting Shuffle phase...\n");
    shuffle(context);
    printf("Shuffle phase completed.\n");
    
    // 3. Reduce阶段：创建reducer线程
    printf("Starting Reduce phase with %d reducers...\n", context->config.num_reducers);
    
    for (int i = 0; i < context->config.num_reducers; i++) {
        ThreadArg *arg = malloc(sizeof(ThreadArg));
        arg->id = i;
        arg->context = context;
        arg->data = NULL;
        arg->data_count = 0;
        
        pthread_create(&context->reducer_threads[i], NULL, reducer_thread_func, arg);
    }
    
    // 等待所有reducer完成
    for (int i = 0; i < context->config.num_reducers; i++) {
        pthread_join(context->reducer_threads[i], NULL);
    }
    printf("Reduce phase completed.\n");
    
    // 4. 输出阶段：合并并写入结果
    printf("Writing final results...\n");
    write_final_results(context);
    
    return 0;
}

// 清理资源
void mapreduce_cleanup(struct MapReduceContext *context) {
    if (!context) return;
    
    // 释放中间结果
    for (int i = 0; i < context->config.num_reducers; i++) {
        struct KeyValue *current = context->intermediate[i];
        while (current) {
            struct KeyValue *next = current->next;
            free(current->key);
            free(current->value);
            free(current);
            current = next;
        }
        
        current = context->final_results[i];
        while (current) {
            struct KeyValue *next = current->next;
            free(current->key);
            free(current->value);
            free(current);
            current = next;
        }
    }
    
    free(context->intermediate);
    free(context->final_results);
    free(context->mapper_threads);
    free(context->reducer_threads);
    
    for (int i = 0; i < context->config.num_reducers; i++) {
        pthread_mutex_destroy(&context->mutexes[i]);
    }
    free(context->mutexes);
    
    free(context);
}