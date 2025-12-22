#ifndef FILE_UTILS_H
#define FILE_UTILS_H

// 函数声明
int file_exists(const char *filename);
long get_file_size(const char *filename);
char* read_file(const char *filename);
int count_lines(const char *filename);

#endif