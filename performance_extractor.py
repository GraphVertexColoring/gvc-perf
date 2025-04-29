import os
import os.path
import csv
import time
import gzip
import shutil
import argparse

def unzip_files_in_directory(directory):
    # Unzips all .gz files in the directory and returns a list of unzipped file paths.
    unzipped_files = []
    for file in os.listdir(directory):
        if file.endswith(".gz"):
            gz_path = os.path.join(directory, file)
            sol_path = os.path.join(directory, file[:-3])  # remove .gz

            with gzip.open(gz_path, 'rb') as f_in:
                with open(sol_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            unzipped_files.append(sol_path)
    return unzipped_files


def get_best(best_solutions_path):
    best_dict = {}

    with open(best_solutions_path, mode='r', encoding = 'utf-8') as file:
        lines = file.readlines()

    table_lines = [line.strip() for line in lines if line.strip().startswith("|")]

    if not table_lines:
        print("No markdown table found in the specified file.")
    else: 
        # first row should be the header
        header = [col.strip() for col in table_lines[0].strip("|").split("|")]

        #skipping the second row as the format means that it is a separator row
        for row_line in table_lines[2:]:
            row_data = [cell.strip() for cell in row_line.strip("|").split("|")]

            #skips any incomplete rows. in case a manual row was made wrong
            if len(row_data) != len(header):
                continue
            
            #creates a mapping for each header to its corresponding cell
            row = dict(zip(header, row_data))
            key = row.pop("Instance")
            best_dict[key] = row
            
    return best_dict
##
# The following script assumes only valid solutions are present in the directories that it gathers performances from.
#   If an invalid solution is present that it will be treated as valid, thus the validation and removal should be handled by a seperate script used upon posting of a solution.
##
def gather_algo_performance(results_dir, feature_dict_path, best_solutions_path, output_csv):
    print("Processing Algorithm results...")

    algo_dict = {}

    # Load existing algoPerf.csv if it exists
    existing_data = {}
    existing_algos = set()
    if os.path.exists(output_csv):
        with open(output_csv, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                instance_name = row['instance_name']
                existing_data[instance_name] = {k: (int(v) if v.isdigit() else v) for k, v in row.items() if k != 'instance_name'}
                existing_algos.update(row.keys())
        existing_algos.difference_update(['instance_name', 'best', 'best_performance'])
    else:
        existing_data = {}
        existing_algos = set()


    # Load best known solutions
    best_dict = get_best(best_solutions_path)

    # Load feature dictionary
    feature_dict = {}
    with open(feature_dict_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            feature_dict[row['feature_source']] = row

    # Identify available algorithms by scanning the results directory
    algos = [d for d in os.listdir(results_dir) if os.path.isdir(os.path.join(results_dir, d))]

    # To handle previously written algorithms in the output CSV
    existing_algos = set()
    if os.path.exists(output_csv):
        with open(output_csv, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                instance_name = row['instance_name']
                # Avoid duplicates by checking existing fields
                algo_dict[instance_name] = {key: (int(value) if value.isdigit() else value) for key, value in row.items() if key != 'instance_name'}
                existing_algos.update(row.keys())

    existing_algos.difference_update(['instance_name', 'best', 'best_performance'])

    all_algos = sorted(existing_algos.union(algos))

    all_temp_unzipped = []

    # Start merging data
    for instance_name in feature_dict:
        # Start from existing data if available
        if instance_name in existing_data:
            algo_dict[instance_name] = existing_data[instance_name] # this is a quick fix and will need changing if working further
        else:
            algo_dict[instance_name] = {}

        # Default 'best_performance'
        if 'best_performance' not in algo_dict[instance_name]:
            algo_dict[instance_name]['best_performance'] = int(feature_dict[instance_name]['feature_num_vertices'])
        
        bestname = instance_name.replace(".col", "")

        # Check best known solution
        if bestname in best_dict:
            algo_dict[instance_name]['best'] = int(best_dict[bestname]['best'])
            algo_dict[instance_name]['best_performance'] = int(best_dict[bestname]['best'])
        else:
            # If no "best", use current best_performance
            algo_dict[instance_name]['best'] = algo_dict[instance_name]['best_performance']

        # Process each algorithm
        for algo in algos:
            algo_path = os.path.join(results_dir, algo)

            # Unzip any gzipped files before checking
            temp_unzipped = unzip_files_in_directory(algo_path)
            all_temp_unzipped.extend(temp_unzipped)

            filename = instance_name.replace(".col", ".sol")
            solution_file_path = os.path.join(algo_path, filename)

            if os.path.isfile(solution_file_path):
                with open(solution_file_path, mode="r") as f:
                    colors = {int(line.strip()) for line in f}
                    chromatic = len(colors)

                    # Update 'best_performance' if this chromatic number is better
                    algo_dict[instance_name]['best_performance'] = min(algo_dict[instance_name]['best_performance'], chromatic)

                    # Always update with the latest chromatic number
                    algo_dict[instance_name][algo] = chromatic
            else:
                # If no solution file is found for this algo, retain existing value or NaN
                if algo not in algo_dict[instance_name]:
                    algo_dict[instance_name][algo] = float('nan')

    # Collect all algorithm names, ensuring they don't duplicate
    all_algos = sorted(existing_algos.union(algos))

    # Write output to CSV
    fieldnames = ['instance_name', 'best', 'best_performance'] + all_algos

    # Write output to CSV
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for instance, data in algo_dict.items():
            row = {'instance_name': instance, **data}
            writer.writerow(row)

    print(f"Results saved to {output_csv}")

    # Cleanup temporary unzipped files
    for temp_file in all_temp_unzipped:
        try:
            os.remove(temp_file)
        except Exception as e:
            print(f"Could not delete {temp_file}: {e}")


def gather_algo_performance_mult(results_dir, feature_dict_path, best_solutions_path, output_csv):
    print("Processing Algorithm results (wide format, nested runs)...")

    algo_dict = {}
    best_dict = {}
    all_temp_unzipped = []

    # Load existing performance if output already exists
    existing_algos = set()
    if os.path.exists(output_csv):
        with open(output_csv, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                instance_name = row['instance_name']
                if instance_name not in algo_dict:
                    algo_dict[instance_name] = {}
                for key, value in row.items():
                    if key == 'instance_name':
                        continue
                    try:
                        algo_dict[instance_name][key] = int(value) if value and value.lower() != 'nan' else float('nan')
                    except ValueError:
                        algo_dict[instance_name][key] = value
                existing_algos.update(row.keys())

    # Load best known solutions
    with open(best_solutions_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row['Instance']
            row.pop('Instance')
            best_dict[key] = row

    # Load feature dictionary
    feature_dict = {}
    with open(feature_dict_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            feature_dict[row['feature_source']] = row

    
    existing_algos.difference_update(['instance_name', 'best', 'best_performance'])

    # Identify runs and algorithms
    run_dirs = sorted([d for d in os.listdir(results_dir) if os.path.isdir(os.path.join(results_dir, d))])
    algos_set = set()

    # Initialize structure for any missing instances
    for instance_name in feature_dict:
        if instance_name not in algo_dict:
            algo_dict[instance_name] = {}
            algo_dict[instance_name]['best_performance'] = int(feature_dict[instance_name]['feature_num_vertices'])

            bestname = instance_name.replace(".col", "")
            if bestname in best_dict:
                algo_dict[instance_name]['best'] = int(best_dict[bestname]['best'])
                algo_dict[instance_name]['best_performance'] = int(best_dict[bestname]['best'])
            else:
                algo_dict[instance_name]['best'] = algo_dict[instance_name]['best_performance']

    # Process each run
    for run_name in run_dirs:
        run_path = os.path.join(results_dir, run_name)
        if not os.path.isdir(run_path):
            continue

        for algo in os.listdir(run_path):
            algo_path = os.path.join(run_path, algo)
            if not os.path.isdir(algo_path):
                continue

            # Unzip any .gz files
            temp_unzipped = unzip_files_in_directory(algo_path)
            all_temp_unzipped.extend(temp_unzipped)

            algos_set.add(algo)

            for instance_name in feature_dict:
                filename = instance_name.replace(".col", ".sol")
                results_subdir = [d for d in os.listdir(algo_path) if d.endswith("_certificates")]
                if results_subdir:
                    results_path = os.path.join(algo_path, results_subdir[0])
                    result_file = os.path.join(results_path, filename)
                else:
                    continue

                key = f"{algo}_{run_name}"

                if os.path.isfile(result_file):
                    with open(result_file, mode="r") as f:
                        colors = {int(line.strip()) for line in f}
                        chromatic = len(colors)

                        if instance_name not in algo_dict:
                            algo_dict[instance_name] = {}
                        algo_dict[instance_name][key] = chromatic
                        if 'best_performance' not in algo_dict[instance_name]:
                            algo_dict[instance_name]['best_performance'] = chromatic
                        else:
                            algo_dict[instance_name]['best_performance'] = min(
                                algo_dict[instance_name]['best_performance'],
                                chromatic
                            )
                else:
                    if instance_name not in algo_dict:
                        algo_dict[instance_name] = {}
                    algo_dict[instance_name][key] = float('nan')

    # Collect all fields dynamically
    run_keys = sorted(run_dirs)
    all_algos = sorted(existing_algos.union({f"{algo}_{run}" for algo in algos_set for run in run_keys}))
    fieldnames = ['instance_name', 'best', 'best_performance'] + all_algos

    # Write updated output
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for instance_name, data in algo_dict.items():
            row = {'instance_name': instance_name}
            for field in fieldnames[1:]:  # Skip 'instance_name'
                row[field] = data.get(field, float('nan'))
            writer.writerow(row)

    print(f"Results saved to {output_csv}")

    # Cleanup temporary unzipped files
    for temp_file in all_temp_unzipped:
        try:
            os.remove(temp_file)
        except Exception as e:
            print(f"Could not delete {temp_file}: {e}")


#modify placement of names here.
# run is only meant to function with the action.
def run(flag):
    start = time.time()

    result_dir = "../Algos/Run1" 
    feature_path = "../coloring/Resources/InstanceFeatures.csv" # this value could remain static.
    best_solutions = "../coloring/Resources/best.csv"           # Could be changed to the markdown 
    output = "../coloring/Resources/algoPerf.csv"
    gather_algo_performance(result_dir, feature_path, best_solutions, output)

    result_dir = "../Algos/"
    best_solutions = "../coloring/Resources/best.csv"
    output = "../coloring/Resources/algoPerfMult.csv"
    gather_algo_performance_mult(result_dir, feature_path, best_solutions, output)

    end = time.time()
    print(end-start)

if __name__ == "__main__":
    run()