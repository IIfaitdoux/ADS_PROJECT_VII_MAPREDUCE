#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// Mapper函数：处理输入文件，发射(word, 1)键值对
void wordcount_mapper(char *input_file, void *context_ptr) {
    FILE *file = fopen(input_file, "r");
    if (!file) {
        fprintf(stderr, "Error opening file: %s\n", input_file);
        return;
    }
    
    char word[256];
    while (fscanf(file, "%255s", word) == 1) {
        // 清理单词：转换为小写，移除标点
        int len = strlen(word);
        int j = 0;
        for (int i = 0; i < len; i++) {
            if (isalpha(word[i])) {
                word[j++] = tolower(word[i]);
            }
        }
        word[j] = '\0';
        
        if (strlen(word) > 0) {
            int *value = malloc(sizeof(int));
            *value = 1;
            emit_intermediate(context_ptr, word, value, sizeof(int));
            free(value);
        }
    }
    
    fclose(file);
}