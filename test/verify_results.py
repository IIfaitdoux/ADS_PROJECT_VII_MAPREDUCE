#!/usr/bin/env python3
"""
Verification Script for MapReduce Word Count
Checks if serial and parallel results are identical
"""

import sys
import os
from collections import defaultdict

def read_results(filename):
    """Read results file, return dictionary {word: count}"""
    results = {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    word = parts[0]
                    try:
                        count = int(parts[1])
                        results[word] = count
                    except ValueError:
                        print(f"  WARNING: Invalid count at line {line_num} in {filename}: {parts[1]}")
                else:
                    print(f"  WARNING: Invalid format at line {line_num} in {filename}: {line}")
    except FileNotFoundError:
        print(f"  ERROR: File not found: {filename}")
        return None
    except Exception as e:
        print(f"  ERROR: Could not read file {filename}: {e}")
        return None
    
    return results

def verify_sorting(filename):
    """Verify that results are sorted correctly"""
    print(f"\nVerifying sorting order in {filename}")
    
    prev_count = None
    prev_word = None
    errors = 0
    line_num = 0
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split()
                if len(parts) < 2:
                    print(f"    ERROR at line {line_num}: Invalid format")
                    errors += 1
                    continue
                    
                word = parts[0]
                try:
                    count = int(parts[1])
                except ValueError:
                    print(f"    ERROR at line {line_num}: Invalid count '{parts[1]}'")
                    errors += 1
                    continue
                
                if prev_count is not None:
                    # Check sorting rules: count descending, then word ascending
                    if count > prev_count:
                        print(f"    ERROR at line {line_num}: Count increased ({prev_count} -> {count})")
                        errors += 1
                    elif count == prev_count and word < prev_word:
                        print(f"    ERROR at line {line_num}: Same count but words not in alphabetical order")
                        print(f"      '{prev_word}' should come before '{word}'")
                        errors += 1
                
                prev_count = count
                prev_word = word
        
        if errors == 0:
            print(f"    ✓ Sorting correct ({line_num} lines)")
            return True
        else:
            print(f"    ✗ Found {errors} sorting errors")
            return False
    except Exception as e:
        print(f"    ERROR: Could not verify sorting: {e}")
        return False

def verify_results(serial_file, parallel_file):
    """Verify that serial and parallel results are identical"""
    print(f"\nVerifying results consistency:")
    print(f"  Serial: {serial_file}")
    print(f"  Parallel: {parallel_file}")
    
    serial_results = read_results(serial_file)
    parallel_results = read_results(parallel_file)
    
    if serial_results is None or parallel_results is None:
        return False
    
    # Check number of unique words
    serial_count = len(serial_results)
    parallel_count = len(parallel_results)
    
    if serial_count != parallel_count:
        print(f"  ✗ Word count mismatch: Serial={serial_count}, Parallel={parallel_count}")
        
        # Find which words are missing
        serial_words = set(serial_results.keys())
        parallel_words = set(parallel_results.keys())
        
        missing_in_parallel = serial_words - parallel_words
        missing_in_serial = parallel_words - serial_words
        
        if missing_in_parallel:
            print(f"    Words in serial but not in parallel (first 5): {list(missing_in_parallel)[:5]}")
        
        if missing_in_serial:
            print(f"    Words in parallel but not in serial (first 5): {list(missing_in_serial)[:5]}")
        
        return False
    else:
        print(f"  ✓ Both files contain {serial_count} unique words")
    
    # Check each word's count
    errors = 0
    mismatch_details = []
    
    for word, serial_count in serial_results.items():
        if word not in parallel_results:
            mismatch_details.append(f"Word '{word}' not found in parallel results")
            errors += 1
        elif parallel_results[word] != serial_count:
            mismatch_details.append(f"Word '{word}': Serial={serial_count}, Parallel={parallel_results[word]}")
            errors += 1
    
    # Check for words in parallel but not in serial
    for word in parallel_results:
        if word not in serial_results:
            mismatch_details.append(f"Word '{word}' not found in serial results")
            errors += 1
    
    if errors == 0:
        print("  ✓ Results are identical!")
        return True
    else:
        print(f"  ✗ Found {errors} mismatches")
        if errors <= 10:
            print("    Mismatch details:")
            for detail in mismatch_details[:10]:
                print(f"      {detail}")
        else:
            print(f"    Showing first 10 of {errors} mismatches:")
            for detail in mismatch_details[:10]:
                print(f"      {detail}")
        return False

def main():
    if len(sys.argv) != 3:
        print("Usage: python verify_results.py <serial_output> <parallel_output>")
        print("Example: python verify_results.py test/serial_output.txt test/parallel_output.txt")
        sys.exit(1)
    
    serial_file = sys.argv[1]
    parallel_file = sys.argv[2]
    
    if not os.path.exists(serial_file):
        print(f"Error: Serial output file '{serial_file}' not found")
        sys.exit(1)
    
    if not os.path.exists(parallel_file):
        print(f"Error: Parallel output file '{parallel_file}' not found")
        sys.exit(1)
    
    print("=" * 70)
    print("MAPREDUCE WORD COUNT - RESULTS VERIFICATION")
    print("=" * 70)
    
    # Verify serial results sorting
    print("\n1. VERIFYING SERIAL OUTPUT:")
    serial_sort_ok = verify_sorting(serial_file)
    
    # Verify parallel results sorting
    print("\n2. VERIFYING PARALLEL OUTPUT:")
    parallel_sort_ok = verify_sorting(parallel_file)
    
    # Verify two versions are consistent
    print("\n3. COMPARING SERIAL AND PARALLEL RESULTS:")
    consistency_ok = verify_results(serial_file, parallel_file)
    
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    
    all_passed = serial_sort_ok and parallel_sort_ok and consistency_ok
    
    if all_passed:
        print("✓ ALL TESTS PASSED!")
        print("- Both outputs are correctly sorted")
        print("- Serial and parallel results are identical")
        sys.exit(0)
    else:
        print("✗ SOME TESTS FAILED:")
        if not serial_sort_ok:
            print("  - Serial output sorting incorrect")
        if not parallel_sort_ok:
            print("  - Parallel output sorting incorrect")
        if not consistency_ok:
            print("  - Serial and parallel results inconsistent")
        
        # Provide debugging tips
        print("\nDEBUGGING TIPS:")
        print("1. Check that both implementations handle words the same way")
        print("2. Verify that sorting comparison functions are identical")
        print("3. Test with smaller datasets to debug more easily")
        print("4. Check for thread safety issues in parallel implementation")
        
        sys.exit(1)

if __name__ == "__main__":
    main()