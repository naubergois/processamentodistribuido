import json
import os

path = "notebooks/04_sales_fraud.ipynb"
submit_cmd = "!spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 kafka_consumer.py"

with open(path, 'r') as f:
    nb = json.load(f)

# Find relevant cells
train_cell_idx = -1
stream_cell_idx = -1

for i, cell in enumerate(nb['cells']):
    source = "".join(cell['source'])
    if "KMeans(k=2" in source and "model.clusterCenters" in source:
        train_cell_idx = i
    if "readStream" in source and ".start()" in source:
        stream_cell_idx = i

if train_cell_idx != -1 and stream_cell_idx != -1:
    print(f"Found Train Cell ({train_cell_idx}) and Stream Cell ({stream_cell_idx})")
    
    # Extract Logic
    train_source = nb['cells'][train_cell_idx]['source']
    stream_source = nb['cells'][stream_cell_idx]['source']
    
    # Filter imports from stream source that might duplicate or be needed?
    # Stream source has: from pyspark.sql.functions ...
    # Train source has: from pyspark.sql ... SparkSession ...
    
    # We will COMBINE them into the Stream Cell, and make the Train Cell just a markdown saying "Moved to consumer script"
    # Or better: Just replace the Stream Cell with EVERYTHING, and leave the Train Cell as is (or it will run twice if user runs all cells).
    # Since we are using %%writefile, correct approach is:
    # 1. Create a merged source code.
    # 2. Put it in the Stream Cell with %%writefile.
    
    merged_source = ["%%writefile kafka_consumer.py\n"]
    
    # Add imports/setup from Train Cell
    for line in train_source:
        merged_source.append(line)
        
    merged_source.append("\n# --- Streaming Logic ---\n")
    
    # Add Streaming Logic
    for line in stream_source:
        if "time.sleep" in line and "query" not in line: continue
        if "query.stop()" in line: continue
        merged_source.append(line)
        
    merged_source.append("\nquery.awaitTermination()")
    
    # Update Stream Cell
    nb['cells'][stream_cell_idx]['source'] = merged_source
    
    # Add Execution Cell if not present
    has_submit = False
    for cell in nb['cells']:
        if cell['cell_type'] == 'code' and submit_cmd in "".join(cell['source']):
            has_submit = True
            
    if not has_submit:
        new_cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [submit_cmd]
        }
        nb['cells'].append(new_cell)
    
    with open(path, 'w') as f:
        json.dump(nb, f, indent=1)
    print("Updated 04_sales_fraud.ipynb")
else:
    print("Could not find cells.")
