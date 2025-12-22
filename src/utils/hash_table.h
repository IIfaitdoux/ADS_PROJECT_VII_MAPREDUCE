#ifndef HASH_TABLE_H
#define HASH_TABLE_H

// 哈希表节点
struct HashTableNode {
    char *key;
    void *value;
    struct HashTableNode *next;
};

// 哈希表结构
struct HashTable {
    int size;
    int count;
    struct HashTableNode **buckets;
};

// 哈希表迭代器
struct HashTableIterator {
    struct HashTable *ht;
    int bucket_index;
    struct HashTableNode *current_node;
    char *key;
    void *value;
};

// 函数声明
struct HashTable* hash_table_create(int size);
void hash_table_insert(struct HashTable *ht, char *key, void *value);
void* hash_table_get(struct HashTable *ht, const char *key);
void hash_table_iter_init(struct HashTableIterator *it, struct HashTable *ht);
int hash_table_iter_next(struct HashTableIterator *it);
int hash_table_size(struct HashTable *ht);
void hash_table_destroy(struct HashTable *ht);
void hash_table_destroy_keep_data(struct HashTable *ht);

#endif