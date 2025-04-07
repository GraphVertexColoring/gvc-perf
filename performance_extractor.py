import os
import os.path
import csv
import time

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

    # Write new performance data
    for instance_name in feature_dict:
        if instance_name not in algo_dict:
            algo_dict[instance_name] = {}

        # Set initial 'best_performance'
        algo_dict[instance_name]['best_performance'] = int(feature_dict[instance_name]['feature_num_vertices'])
        bestname = instance_name.replace(".col", "")
        filename = instance_name.replace(".col", ".sol")

        # Check for best solution from the markdown file
        if bestname in best_dict:
            algo_dict[instance_name]['best'] = int(best_dict[bestname]['best'])
            algo_dict[instance_name]['best_performance'] = int(best_dict[bestname]['best'])

        # Process each algorithm directory in 'results_dir'
        for algo in algos:
            algo_path = os.path.join(results_dir, algo)

            # Check if the solution file exists for the instance in the current algorithm's folder
            if filename in os.listdir(algo_path):
                with open(os.path.join(algo_path, filename), mode="r") as f:
                    # Collect the solution's chromatic number (unique vertex count)
                    colors = {int(line.strip()) for line in f}
                    chromatic = len(colors)

                    # Update the best performance
                    algo_dict[instance_name]['best_performance'] = min(algo_dict[instance_name]['best_performance'], chromatic)
                    algo_dict[instance_name][algo] = chromatic
            else:
                # If no solution file is found for this algorithm, mark it as NaN
                algo_dict[instance_name][algo] = float('nan')

        # If bestname isn't found in the markdown file, use the calculated 'best_performance'
        if bestname not in best_dict:
            algo_dict[instance_name]['best'] = algo_dict[instance_name]['best_performance']

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


def run():
    start = time.time()
    result_dir = "../Resources/solutions"  
    feature_path = "../Resources/InstanceFeatures.csv" # this value could remain static.
    best_solutions = "../docs/best/best_solutions.md" 
    output = "../Resources/algoPerf.csv"
    gather_algo_performance(result_dir, feature_path, best_solutions, output)

    end = time.time()
    print(end-start)

run() # might be fine for now, could have this remain like this, and just be completely static..

