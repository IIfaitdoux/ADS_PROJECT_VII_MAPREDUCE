#include "hash_table.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// 哈希函数（djb2算法）
unsigned long hash_function(const char *str) {
    unsigned long hash = 5381;
    int c;
    
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    
    return hash;
}

// 创建哈希表
struct HashTable* hash_table_create(int size) {
    struct HashTable *ht = malloc(sizeof(struct HashTable));
    if (!ht) return NULL;
    
    ht->size = size;
    ht->count = 0;
    ht->buckets = calloc(size, sizeof(struct HashTableNode*));
    
    return ht;
}

// 插入键值对
void hash_table_insert(struct HashTable *ht, char *key, void *value) {
    unsigned long index = hash_function(key) % ht->size;
    
    // 创建新节点
    struct HashTableNode *new_node = malloc(sizeof(struct HashTableNode));
    new_node->key = key;
    new_node->value = value;
    new_node->next = NULL;
    
    // 插入到链表头部
    new_node->next = ht->buckets[index];
    ht->buckets[index] = new_node;
    ht->count++;
}

// 查找键值对
void* hash_table_get(struct HashTable *ht, const char *key) {
    unsigned long index = hash_function(key) % ht->size;
    
    struct HashTableNode *current = ht->buckets[index];
    while (current) {
        if (strcmp(current->key, key) == 0) {
            return current->value;
        }
        current = current->next;
    }
    
    return NULL;
}

// 初始化迭代器
void hash_table_iter_init(struct HashTableIterator *it, struct HashTable *ht) {
    it->ht = ht;
    it->bucket_index = 0;
    it->current_node = NULL;
    it->key = NULL;
    it->value = NULL;
}

// 获取下一个键值对
int hash_table_iter_next(struct HashTableIterator *it) {
    // 如果当前节点有下一个，直接返回
    if (it->current_node && it->current_node->next) {
        it->current_node = it->current_node->next;
        it->key = it->current_node->key;
        it->value = it->current_node->value;
        return 1;
    }
    
    // 否则寻找下一个非空桶
    while (it->bucket_index < it->ht->size) {
        it->current_node = it->ht->buckets[it->bucket_index];
        it->bucket_index++;
        
        if (it->current_node) {
            it->key = it->current_node->key;
            it->value = it->current_node->value;
            return 1;
        }
    }
    
    return 0; // 没有更多元素
}

// 获取哈希表大小
int hash_table_size(struct HashTable *ht) {
    return ht->count;
}

// 销毁哈希表（释放所有内存）
void hash_table_destroy(struct HashTable *ht) {
    for (int i = 0; i < ht->size; i++) {
        struct HashTableNode *current = ht->buckets[i];
        while (current) {
            struct HashTableNode *next = current->next;
            free(current->key);
            free(current->value);
            free(current);
            current = next;
        }
    }
    
    free(ht->buckets);
    free(ht);
}

// 销毁哈希表（不释放key和value）
void hash_table_destroy_keep_data(struct HashTable *ht) {
    for (int i = 0; i < ht->size; i++) {
        struct HashTableNode *current = ht->buckets[i];
        while (current) {
            struct HashTableNode *next = current->next;
            free(current);
            current = next;
        }
    }
    
    free(ht->buckets);
    free(ht);
}