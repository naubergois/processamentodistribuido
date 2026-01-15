import json
import os
import glob

notebooks_dir = "notebooks"
notebooks = glob.glob(os.path.join(notebooks_dir, "*.ipynb"))

new_source = [
    "# Instalar Java\n",
    "!apt-get install openjdk-8-jdk-headless -qq > /dev/null\n",
    "\n",
    "# Baixar e Instalar Spark\n",
    "!wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && tar xf spark-3.5.0-bin-hadoop3.tgz\n",
    "\n",
    "# Baixar e Instalar Kafka\n",
    "!wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar xf kafka_2.13-3.6.1.tgz\n",
    "\n",
    "# Instalar pacotes Python\n",
    "!pip install -q findspark pyspark kafka-python"
]

def update_notebook(path):
    with open(path, 'r') as f:
        nb = json.load(f)
    
    updated = False
    
    # Strategy 1: Find valid cell and replace
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            src = "".join(cell['source'])
            # Loose matching for previous install commands
            if "openjdk" in src or "spark-3.5.0" in src:
                cell['source'] = new_source
                updated = True
                print(f"Updated existing cell in {path}")
                break
    
    # Strategy 2: If not found, insert at appropriate place
    if not updated:
        # Find index to insert. Usually after markdown "Config..."
        insert_idx = 0
        for i, cell in enumerate(nb['cells']):
            if cell['cell_type'] == 'markdown':
                if "Configura" in "".join(cell['source']) or "Environment" in "".join(cell['source']):
                    insert_idx = i + 1
                    break
        
        # If no markdown found, just put at 0 or 1
        if insert_idx == 0 and len(nb['cells']) > 0:
            insert_idx = 1
            
        new_cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": new_source
        }
        nb['cells'].insert(insert_idx, new_cell)
        updated = True
        print(f"Inserted new cell in {path} at index {insert_idx}")

    if updated:
        with open(path, 'w') as f:
            json.dump(nb, f, indent=1)

for nb_path in notebooks:
    try:
        update_notebook(nb_path)
    except Exception as e:
        print(f"Error updating {nb_path}: {e}")
