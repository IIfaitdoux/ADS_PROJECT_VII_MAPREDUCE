#!/usr/bin/env python3
"""
Performance Test Script for MapReduce Word Count
Compares serial vs parallel performance
"""

import subprocess
import time
import os
import sys
import platform
import csv
from datetime import datetime

def run_command(cmd, description=""):
    """Run command and return execution time and output"""
    print(f"  Running: {cmd}")
    
    start_time = time.time()
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        if result.returncode != 0:
            print(f"    ERROR: Command failed with return code {result.returncode}")
            if result.stderr:
                print(f"    Error output: {result.stderr[:200]}...")
            return None, None
        
        return execution_time, result.stdout
        
    except Exception as e:
        print(f"    ERROR: Exception while running command: {e}")
        return None, None

def test_serial(input_file, output_file):
    """Test serial version"""
    cmd = f"serial_wc.exe {output_file} {input_file}" if platform.system() == "Windows" else f"./serial_wc {output_file} {input_file}"
    return run_command(cmd, "Serial version")

def test_parallel(input_file, output_file):
    """Test parallel version"""
    cmd = f"parallel_wc.exe {output_file} {input_file}" if platform.system() == "Windows" else f"./parallel_wc {output_file} {input_file}"
    return run_command(cmd, "Parallel version")

def get_file_stats(filename):
    """Get file statistics"""
    try:
        if os.path.exists(filename):
            size = os.path.getsize(filename)
            with open(filename, 'r', encoding='utf-8') as f:
                lines = sum(1 for _ in f)
            return size, lines
        return 0, 0
    except:
        return 0, 0

def main():
    print("=" * 70)
    print("MAPREDUCE WORD COUNT - PERFORMANCE TEST")
    print("=" * 70)
    
    # Detect operating system
    system = platform.system()
    print(f"Operating System: {system}")
    print(f"Python Version: {platform.python_version()}")
    print(f"Current Directory: {os.getcwd()}")
    print()
    
    # Check if executables exist
    if system == "Windows":
        serial_exe = "serial_wc.exe"
        parallel_exe = "parallel_wc.exe"
        generate_exe = "generate_test_data.exe"
    else:
        serial_exe = "./serial_wc"
        parallel_exe = "./parallel_wc"
        generate_exe = "./generate_test_data"
    
    print("Checking for required executables...")
    missing_executables = []
    
    if not os.path.exists(serial_exe):
        missing_executables.append(serial_exe)
    if not os.path.exists(parallel_exe):
        missing_executables.append(parallel_exe)
    if not os.path.exists(generate_exe):
        missing_executables.append(generate_exe)
    
    if missing_executables:
        print(f"ERROR: Missing executables: {', '.join(missing_executables)}")
        print("Please compile the programs first:")
        print("  On Windows: gcc -o serial_wc.exe src/serial_wordcount.c ...")
        print("  On Linux/macOS: make all")
        sys.exit(1)
    
    print("✓ All executables found")
    print()
    
    # Create test directory if it doesn't exist
    if not os.path.exists("test"):
        os.makedirs("test")
        print("Created test/ directory")
    
    # Test configurations
    test_configs = [
        {"name": "Small", "words": 1000, "runs": 3},
        {"name": "Medium", "words": 5000, "runs": 3},
        {"name": "Large", "words": 10000, "runs": 3},
        {"name": "Extra Large", "words": 50000, "runs": 2},
    ]
    
    # Performance results storage
    performance_results = []
    
    print("STARTING PERFORMANCE TESTS")
    print("-" * 70)
    
    for config in test_configs:
        test_name = config["name"]
        word_count = config["words"]
        num_runs = config["runs"]
        
        print(f"\nTEST: {test_name} dataset ({word_count:,} words)")
        print("-" * 40)
        
        # Generate test data
        test_filename = f"test/test_{word_count}.txt"
        
        if not os.path.exists(test_filename):
            print(f"  Generating test data: {word_count} words")
            gen_cmd = f"{generate_exe} {test_filename} {word_count}"
            gen_time, _ = run_command(gen_cmd, "Generate test data")
            
            if gen_time is None:
                print(f"  WARNING: Failed to generate test data for {word_count} words")
                continue
                
            file_size, _ = get_file_stats(test_filename)
            print(f"  Generated: {test_filename} ({file_size:,} bytes)")
        else:
            file_size, _ = get_file_stats(test_filename)
            print(f"  Using existing: {test_filename} ({file_size:,} bytes)")
        
        # Initialize timing variables
        serial_times = []
        parallel_times = []
        
        # Run tests
        for run_num in range(1, num_runs + 1):
            print(f"\n  Run {run_num}/{num_runs}")
            print("  " + "-" * 30)
            
            # Serial test
            serial_output = f"test/serial_{word_count}_run{run_num}.txt"
            serial_time, serial_output_text = test_serial(test_filename, serial_output)
            
            if serial_time is not None:
                serial_times.append(serial_time)
                serial_unique = 0
                if os.path.exists(serial_output):
                    with open(serial_output, 'r', encoding='utf-8') as f:
                        serial_unique = sum(1 for _ in f)
                print(f"    Serial: {serial_time:.3f}s, Unique words: {serial_unique}")
            
            # Parallel test
            parallel_output = f"test/parallel_{word_count}_run{run_num}.txt"
            parallel_time, parallel_output_text = test_parallel(test_filename, parallel_output)
            
            if parallel_time is not None:
                parallel_times.append(parallel_time)
                parallel_unique = 0
                if os.path.exists(parallel_output):
                    with open(parallel_output, 'r', encoding='utf-8') as f:
                        parallel_unique = sum(1 for _ in f)
                print(f"    Parallel: {parallel_time:.3f}s, Unique words: {parallel_unique}")
        
        # Calculate averages
        if serial_times and parallel_times:
            avg_serial = sum(serial_times) / len(serial_times)
            avg_parallel = sum(parallel_times) / len(parallel_times)
            
            if avg_parallel > 0:
                speedup = avg_serial / avg_parallel
            else:
                speedup = 0
            
            print(f"\n  Average Results for {test_name}:")
            print(f"    Serial:   {avg_serial:.3f}s")
            print(f"    Parallel: {avg_parallel:.3f}s")
            print(f"    Speedup:  {speedup:.2f}x")
            
            if speedup > 1:
                print(f"    Parallel is {speedup:.1f}x faster")
            elif speedup < 1:
                print(f"    Serial is {1/speedup:.1f}x faster")
            else:
                print("    Both versions have similar performance")
            
            # Store results
            performance_results.append({
                "Dataset": test_name,
                "Words": word_count,
                "File_Size_Bytes": file_size,
                "Serial_Time_Avg": round(avg_serial, 3),
                "Parallel_Time_Avg": round(avg_parallel, 3),
                "Speedup": round(speedup, 2),
                "Serial_Times": serial_times,
                "Parallel_Times": parallel_times,
            })
    
    # Print summary
    print("\n" + "=" * 70)
    print("PERFORMANCE TEST SUMMARY")
    print("=" * 70)
    
    if not performance_results:
        print("No performance results collected")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("RESULTS TABLE")
    print("=" * 70)
    print(f"{'Dataset':<15} {'Words':<10} {'Size (KB)':<12} {'Serial (s)':<12} {'Parallel (s)':<12} {'Speedup':<10}")
    print("-" * 70)
    
    for result in performance_results:
        size_kb = result["File_Size_Bytes"] / 1024
        print(f"{result['Dataset']:<15} {result['Words']:<10,} {size_kb:<12.1f} {result['Serial_Time_Avg']:<12.3f} {result['Parallel_Time_Avg']:<12.3f} {result['Speedup']:<10.2f}")
    
    # Save results to CSV
    csv_filename = "test/performance_results.csv"
    try:
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = ['Dataset', 'Words', 'File_Size_Bytes', 'Serial_Time_Avg', 'Parallel_Time_Avg', 'Speedup']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in performance_results:
                writer.writerow({
                    'Dataset': result['Dataset'],
                    'Words': result['Words'],
                    'File_Size_Bytes': result['File_Size_Bytes'],
                    'Serial_Time_Avg': result['Serial_Time_Avg'],
                    'Parallel_Time_Avg': result['Parallel_Time_Avg'],
                    'Speedup': result['Speedup']
                })
        
        print(f"\n✓ Results saved to: {csv_filename}")
        
        # Save detailed results
        detailed_filename = "test/performance_detailed.txt"
        with open(detailed_filename, 'w') as f:
            f.write("MapReduce Word Count - Detailed Performance Results\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Test date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"System: {platform.system()} {platform.release()}\n\n")
            
            for result in performance_results:
                f.write(f"Dataset: {result['Dataset']} ({result['Words']:,} words)\n")
                f.write(f"  File size: {result['File_Size_Bytes']:,} bytes\n")
                f.write(f"  Serial times: {result['Serial_Times']}\n")
                f.write(f"  Parallel times: {result['Parallel_Times']}\n")
                f.write(f"  Average serial: {result['Serial_Time_Avg']:.3f}s\n")
                f.write(f"  Average parallel: {result['Parallel_Time_Avg']:.3f}s\n")
                f.write(f"  Speedup: {result['Speedup']:.2f}x\n")
                f.write("\n")
        
        print(f"✓ Detailed results saved to: {detailed_filename}")
        
    except Exception as e:
        print(f"\n✗ Could not save results to file: {e}")
    
    # Analysis and recommendations
    print("\n" + "=" * 70)
    print("ANALYSIS AND RECOMMENDATIONS")
    print("=" * 70)
    
    if len(performance_results) > 1:
        print("\nPerformance Trends:")
        
        # Calculate average speedup across all tests
        speedups = [r['Speedup'] for r in performance_results]
        avg_speedup = sum(speedups) / len(speedups)
        
        print(f"  Average speedup across all tests: {avg_speedup:.2f}x")
        
        # Check if parallelization is effective
        if avg_speedup > 1.1:
            print("  ✓ Parallel implementation is effective")
        elif avg_speedup > 0.9:
            print("  ⚠ Parallel and serial implementations have similar performance")
            print("    Consider optimizing thread management or I/O operations")
        else:
            print("  ⚠ Serial implementation is faster than parallel")
            print("    Possible issues:")
            print("    - Thread creation overhead")
            print("    - Synchronization bottlenecks")
            print("    - Small dataset size")
            print("    - I/O limitations")
    
    print("\nNext steps:")
    print("1. Review the CSV file for detailed results")
    print("2. Test with even larger datasets (>100,000 words)")
    print("3. Experiment with different thread counts in parallel implementation")
    print("4. Profile the code to identify bottlenecks")
    print("5. Compare with theoretical expectations")
    
    print("\n" + "=" * 70)
    print("PERFORMANCE TEST COMPLETED")
    print("=" * 70)

if __name__ == "__main__":
    main()