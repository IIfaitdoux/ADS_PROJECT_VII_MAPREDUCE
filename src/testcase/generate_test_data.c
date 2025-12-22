#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <ctype.h>

#define NUM_WORDS 100000
#define MAX_WORD_LEN 10
#define WORDS_PER_LINE 10

// 随机单词列表（实际中可以从字典读取）
const char *word_list[] = {
    "the", "and", "to", "of", "in", "is", "it", "that", "for", "with",
    "as", "on", "be", "at", "by", "this", "have", "from", "or", "one",
    "had", "by", "word", "but", "not", "what", "all", "were", "we", "when",
    "your", "can", "said", "there", "use", "each", "which", "she", "do",
    "how", "their", "if", "will", "up", "other", "about", "out", "many",
    "then", "them", "these", "so", "some", "her", "would", "make", "like",
    "him", "into", "time", "has", "look", "two", "more", "write", "go", "see",
    "number", "no", "way", "could", "people", "my", "than", "first", "water",
    "been", "call", "who", "oil", "its", "now", "find", "long", "down", "day",
    "did", "get", "come", "made", "may", "part"
};
#define WORD_LIST_SIZE (sizeof(word_list) / sizeof(word_list[0]))

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <output_file> <num_words>\n", argv[0]);
        return 1;
    }
    
    const char *output_file = argv[1];
    int num_words = atoi(argv[2]);
    
    if (num_words < 1000) {
        num_words = 100000; // 默认10万单词
    }
    
    srand(time(NULL));
    
    FILE *file = fopen(output_file, "w");
    if (!file) {
        perror("Error opening output file");
        return 1;
    }
    
    printf("Generating %d words to %s...\n", num_words, output_file);
    
    int words_on_line = 0;
    for (int i = 0; i < num_words; i++) {
        // 随机选择单词，添加一些变体
        const char *base_word = word_list[rand() % WORD_LIST_SIZE];
        
        // 有时添加后缀或改变大小写
        char word[MAX_WORD_LEN + 3];
        strcpy(word, base_word);
        
        // 随机添加复数形式
        if (rand() % 5 == 0 && strlen(word) < MAX_WORD_LEN - 1) {
            strcat(word, "s");
        }
        
        // 随机大写
        if (rand() % 10 == 0) {
            word[0] = toupper(word[0]);
        }
        
        // 添加标点（有时）
        char final_word[MAX_WORD_LEN + 5];
        strcpy(final_word, word);
        if (rand() % 20 == 0) {
            strcat(final_word, ",");
        } else if (rand() % 30 == 0) {
            strcat(final_word, ".");
        }
        
        fprintf(file, "%s", final_word);
        words_on_line++;
        
        if (words_on_line >= WORDS_PER_LINE || i == num_words - 1) {
            fprintf(file, "\n");
            words_on_line = 0;
        } else {
            fprintf(file, " ");
        }
        
        // 显示进度
        if (i % 10000 == 0) {
            printf("Generated %d words...\n", i);
        }
    }
    
    fclose(file);
    printf("Generated %d words in %s\n", num_words, output_file);
    
    return 0;
}