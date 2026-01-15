import json
import os

spark_notebooks = [
    "01_aviation_telemetry.ipynb", "03_healthcare_vitals.ipynb", "04_sales_fraud.ipynb",
    "06_finance_stocks.ipynb", "07_social_sentiment.ipynb", "09_smart_grid.ipynb",
    "11_cybersecurity_intrusion.ipynb", "13_gaming_leaderboard.ipynb", 
    "15_retail_inventory.ipynb", "17_ride_sharing.ipynb", "19_insurance_risk.ipynb"
]

base_dir = "notebooks"
submit_cmd = "!spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 kafka_consumer.py"

def update_notebook(path):
    with open(path, 'r') as f:
        nb = json.load(f)
    
    modified = False
    
    # 1. Inspect Cells
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source_list = cell['source']
            source_text = "".join(source_list)
            
            # Identify Consumer Cell (SparkSession + readStream)
            if "SparkSession.builder" in source_text and "readStream" in source_text:
                if "%%writefile kafka_consumer.py" in source_text:
                    continue # Already updated
                
                print(f"Updating Consumer Cell in {path}")
                
                # Check for query start
                if ".start()" in source_text:
                    # Logic to rewrite:
                    # 1. Add magic
                    # 2. Remove sleep/stop
                    # 3. Ensure awaitTermination
                    
                    new_source = ["%%writefile kafka_consumer.py\n"]
                    
                    for line in source_list:
                        # Skip sleep loop/stop logic used for interactive demo
                        if "time.sleep" in line and "query" not in line: 
                            continue 
                        if "query.stop()" in line: 
                            continue
                        new_source.append(line)
                    
                    # Add awaitTermination if missing
                    has_await = any("awaitTermination" in line for line in new_source)
                    if not has_await:
                        new_source.append("\nquery.awaitTermination()")
                        
                    cell['source'] = new_source
                    modified = True

    # 2. Add Execution Cell if needed
    if modified:
        has_submit = False
        for cell in nb['cells']:
            if cell['cell_type'] == 'code':
                if submit_cmd in "".join(cell['source']):
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
            print(f"Added submit cell to {path}")
    
    if modified:
        with open(path, 'w') as f:
            json.dump(nb, f, indent=1)

for nb_name in spark_notebooks:
    update_notebook(os.path.join(base_dir, nb_name))
