#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

#define DEFAULT_NUM_MAPPERS 4
#define DEFAULT_NUM_REDUCERS 4

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <output_file> <input_file1> [input_file2 ...]\n", argv[0]);
        return 1;
    }
    
    // 准备配置
    struct MapReduceConfig config = {
        .num_mappers = DEFAULT_NUM_MAPPERS,
        .num_reducers = DEFAULT_NUM_REDUCERS,
        .input_files = &argv[2],
        .num_input_files = argc - 2,
        .output_file = argv[1]
    };
    
    // 创建MapReduce上下文
    struct MapReduceContext *context = mapreduce_init(config);
    if (!context) {
        fprintf(stderr, "Error initializing MapReduce context\n");
        return 1;
    }
    
    // 运行MapReduce
    int result = mapreduce_run(context);
    
    if (result == 0) {
        printf("MapReduce job completed successfully\n");
        printf("Output written to %s\n", argv[1]);
    } else {
        fprintf(stderr, "MapReduce job failed with error %d\n", result);
    }
    
    // 清理
    mapreduce_cleanup(context);
    
    return result;
}