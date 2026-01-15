import json

nb_path = "notebooks/04_sales_fraud.ipynb"

with open(nb_path, "r") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source_str = "".join(cell["source"])
        new_source = []
        
        # 1. Update Producer Cell
        if "def generate_transactions():" in source_str:
            print("Updating Producer...")
            for line in cell["source"]:
                line = line.replace("range(100)", "range(3000)")  # Increase quantity
                line = line.replace("time.sleep(0.5)", "time.sleep(0.1)") # Faster generation
                new_source.append(line)
            cell["source"] = new_source

        # 2. Update Consumer Cell (switch to earliest)
        elif "%%writefile kafka_consumer.py" in source_str:
            print("Updating Consumer...")
            for line in cell["source"]:
                if '"startingOffsets", "latest"' in line:
                    line = line.replace('"latest"', '"earliest"')
                    print("Switched offset to earliest")
                new_source.append(line)
            cell["source"] = new_source

with open(nb_path, "w") as f:
    json.dump(nb, f, indent=1)
