import json
import glob

notebooks = glob.glob("notebooks/*.ipynb")

for nb_path in notebooks:
    with open(nb_path, "r") as f:
        nb = json.load(f)
    
    modified = False
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            source = "".join(cell["source"])
            # Update legacy or mismatching versions
            if "spark-sql-kafka-0-10_2.12:3.2.1" in source:
                print(f"Updating version in {nb_path}")
                # Replace line-by-line to preserve structure
                new_source = []
                for line in cell["source"]:
                    new_source.append(line.replace("3.2.1", "3.5.0"))
                cell["source"] = new_source
                modified = True
            
            # Also check old install commands if any persist (e.g. 3.5.0-bin-hadoop3 is fine, but check packages)
            if "spark.jars.packages" in source and "3.2.1" in source:
                 print(f"Updating config version in {nb_path}")
                 new_source = []
                 for line in cell["source"]:
                    new_source.append(line.replace("3.2.1", "3.5.0"))
                 cell["source"] = new_source
                 modified = True

    if modified:
        with open(nb_path, "w") as f:
            json.dump(nb, f, indent=1)
