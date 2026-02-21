import os

def sort_workload():
    # 1. Define Paths
    input_path = '/home/cc/LSMMemoryProfiling/.result/skiplist_compare_feb11_nothrottling-lowpri_true-I100000-U0-Q0-S0-Y0-T5-P131072-B32-E128/workload.txt'
    
    # The output directory requested
    output_dir = '/home/cc/LSMMemoryProfiling/.result/sorted_workload'
    # The final filename
    output_path = os.path.join(output_dir, 'workload.txt')

    print(f"Reading from: {input_path}")

    try:
        # 2. Read the file
        with open(input_path, 'r') as f:
            lines = f.readlines()
        
        print(f"Loaded {len(lines)} rows. Sorting...")

        # 3. Sort the lines
        # Lambda function splits the line by space and selects index 1 (the Key)
        # Structure: [0]=Operation, [1]=Key, [2]=Value
        lines.sort(key=lambda line: line.split()[1])

        # 4. Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # 5. Write the sorted file
        with open(output_path, 'w') as f:
            f.writelines(lines)

        print(f"Success! Sorted workload saved to: {output_path}")
        
        # Optional: Print first 3 lines to verify
        print("\nFirst 3 sorted lines preview:")
        for line in lines[:3]:
            print(line.strip())

    except FileNotFoundError:
        print(f"Error: The file at {input_path} was not found.")
    except IndexError:
        print("Error: A line in the file did not have enough columns to sort by.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    sort_workload()