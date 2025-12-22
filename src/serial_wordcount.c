#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "utils/hash_table.h"
#include "utils/sort.h"

#define MAX_WORD_LENGTH 100
#define HASH_TABLE_SIZE 10000

// 单词计数结构
struct WordCount {
    char word[MAX_WORD_LENGTH];
    int count;
    struct WordCount *next;
};

// 比较函数：先按计数降序，再按单词字典序
int compare_word_count(const void *a, const void *b) {
    struct WordCount *wc1 = (struct WordCount *)a;
    struct WordCount *wc2 = (struct WordCount *)b;
    
    if (wc1->count != wc2->count) {
        return wc2->count - wc1->count; // 降序
    }
    return strcmp(wc1->word, wc2->word); // 字典序升序
}

// 处理单个文件
void process_file(const char *filename, struct HashTable *ht) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Error opening file: %s\n", filename);
        return;
    }
    
    char word[MAX_WORD_LENGTH];
    while (fscanf(file, "%99s", word) == 1) {
        // 转换为小写并清理标点
        for (int i = 0; word[i]; i++) {
            word[i] = tolower(word[i]);
            if (!isalpha(word[i])) {
                word[i] = '\0';
                break;
            }
        }
        
        if (strlen(word) > 0) {
            int *count = hash_table_get(ht, word);
            if (count) {
                (*count)++;
            } else {
                int *new_count = malloc(sizeof(int));
                *new_count = 1;
                hash_table_insert(ht, strdup(word), new_count);
            }
        }
    }
    
    fclose(file);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <output_file> <input_file1> [input_file2 ...]\n", argv[0]);
        return 1;
    }
    
    struct HashTable *ht = hash_table_create(HASH_TABLE_SIZE);
    
    // 处理所有输入文件
    for (int i = 2; i < argc; i++) {
        printf("Processing file: %s\n", argv[i]);
        process_file(argv[i], ht);
    }
    
    // 收集结果到数组
    int num_words = hash_table_size(ht);
    struct WordCount *word_counts = malloc(num_words * sizeof(struct WordCount));
    
    struct HashTableIterator it;
    hash_table_iter_init(&it, ht);
    int idx = 0;
    
    while (hash_table_iter_next(&it)) {
        strncpy(word_counts[idx].word, it.key, MAX_WORD_LENGTH - 1);
        word_counts[idx].word[MAX_WORD_LENGTH - 1] = '\0';
        word_counts[idx].count = *(int *)it.value;
        idx++;
    }
    
    // 排序
    qsort(word_counts, num_words, sizeof(struct WordCount), compare_word_count);
    
    // 输出结果
    FILE *output = fopen(argv[1], "w");
    if (!output) {
        fprintf(stderr, "Error opening output file: %s\n", argv[1]);
        return 1;
    }
    
    for (int i = 0; i < num_words; i++) {
        fprintf(output, "%s %d\n", word_counts[i].word, word_counts[i].count);
    }
    
    fclose(output);
    printf("Results written to %s\n", argv[1]);
    printf("Total unique words: %d\n", num_words);
    
    // 清理
    hash_table_destroy(ht);
    free(word_counts);
    
    return 0;
}