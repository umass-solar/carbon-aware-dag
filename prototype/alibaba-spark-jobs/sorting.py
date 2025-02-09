import csv
import re

def extract_sort_key(task_name):
    """Extract the numeric part of the task_name after the first letter."""
    match = re.search(r'[A-Z](\d+)', task_name)
    return int(match.group(1)) if match else float('inf')  # Handle missing matches gracefully

# Read and sort the CSV file
input_file = 'batch_task.csv' # this file is omitted to avoid Git's file size limit --
# the original can be downloaded from alibaba/cluster-traces repo.
output_file = 'sorted_batch_task.csv'

with open(input_file, 'r') as infile:
    reader = csv.reader(infile)
    rows = list(reader)
    # if row starts with "task_" or "MergeTask", skip it
    rows = [row for row in rows if not row[0].startswith("task_") and not row[0].startswith("MergeTask")]

# Sort rows based on the task_name column (index 0 in your case)
sorted_rows = sorted(rows, key=lambda row: extract_sort_key(row[0]))

# within sorted_rows, only take unique rows (first occurence of each task_name)
unique_task_names = set()
unique_sorted_rows = []
for row in sorted_rows:
    if row[0] not in unique_task_names:
        unique_task_names.add(row[0])
        unique_sorted_rows.append(row)

# Write unique sorted rows back to a new CSV file
with open(output_file, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerows(unique_sorted_rows)

print(f"Unique sorted rows have been written to {output_file}.")
