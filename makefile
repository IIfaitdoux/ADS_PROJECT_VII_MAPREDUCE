# Makefile for Windows (PowerShell)

CC = gcc
CFLAGS = -Wall -std=c99 -O2 -I. -I./src
LDFLAGS = -lm
PTHREAD_FLAGS = -pthread

# 目标文件
SERIAL_TARGET = serial_wc.exe
PARALLEL_TARGET = parallel_wc.exe
GENERATE_TARGET = generate_test_data.exe

# 源文件
SERIAL_SRC = src/serial_wordcount.c src/utils/hash_table.c src/utils/sort.c src/utils/file_utils.c
PARALLEL_SRC = src/parallel_wordcount.c src/mapreduce.c src/utils/hash_table.c src/utils/sort.c src/utils/file_utils.c
GENERATE_SRC = src/testcase/generate_test_data.c

# 默认目标
all: $(SERIAL_TARGET) $(PARALLEL_TARGET) $(GENERATE_TARGET)

# 串行版本
$(SERIAL_TARGET): $(SERIAL_SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# 并行版本
$(PARALLEL_TARGET): $(PARALLEL_SRC)
	$(CC) $(CFLAGS) $(PTHREAD_FLAGS) -o $@ $^ $(LDFLAGS)

# 测试数据生成器
$(GENERATE_TARGET): $(GENERATE_SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# 创建测试目录
testdir:
	-if not exist "test" mkdir test

# 生成测试数据
testdata: $(GENERATE_TARGET) testdir
	@echo "Generating test data..."
	.\generate_test_data.exe test\test_data_10000.txt 10000
	.\generate_test_data.exe test\test_data_50000.txt 50000
	.\generate_test_data.exe test\test_data_100000.txt 100000
	.\generate_test_data.exe test\test_data_200000.txt 200000
	@echo "Test data generated in 'test' directory."

# 运行简单测试
test: all testdata
	@echo "Execute simple testcase with 100,000 words:"
	@echo "1. run serial version..."
	.\serial_wc.exe test\serial_output_100000.txt test\test_data_100000.txt
	@echo "2. run parallel version..."
	.\parallel_wc.exe test\parallel_output_100000.txt test\test_data_100000.txt
	@echo "3. verify result consistency..."
	python test\verify_results.py test\serial_output_100000.txt test\parallel_output_100000.txt

# 清理
clean:
	del /Q $(SERIAL_TARGET) $(PARALLEL_TARGET) $(GENERATE_TARGET) 2>nul
	del /Q *.o 2>nul
	del /Q test\*.txt test\*.csv test\*.png 2>nul

# 完全清理
cleanall: clean
	del /Q serial_wc.exe parallel_wc.exe generate_test_data.exe 2>nul

.PHONY: all testdir testdata test clean cleanall