#include "file_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// 检查文件是否存在
int file_exists(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (file) {
        fclose(file);
        return 1;
    }
    return 0;
}

// 获取文件大小
long get_file_size(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) return -1;
    
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fclose(file);
    
    return size;
}

// 读取文件内容到字符串
char* read_file(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) return NULL;
    
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    char *content = malloc(size + 1);
    if (!content) {
        fclose(file);
        return NULL;
    }
    
    fread(content, 1, size, file);
    content[size] = '\0';
    
    fclose(file);
    return content;
}

// 统计文件行数
int count_lines(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) return -1;
    
    int count = 0;
    char buffer[1024];
    
    while (fgets(buffer, sizeof(buffer), file)) {
        count++;
    }
    
    fclose(file);
    return count;
}