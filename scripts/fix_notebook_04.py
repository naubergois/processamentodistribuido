import json

nb_path = "notebooks/04_sales_fraud.ipynb"

with open(nb_path, "r") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source = "".join(cell["source"])
        
        # 1. Fix Imports in %%writefile cell
        if "%%writefile kafka_consumer.py" in source:
            new_source = []
            for line in cell["source"]:
                if "from pyspark.sql.types import DoubleType" in line:
                    new_source.append("from pyspark.sql.types import DoubleType, StructType, StructField\n")
                else:
                    new_source.append(line)
            cell["source"] = new_source
            print("Fixed imports in 04_sales_fraud.ipynb")

        # 2. Fix Spark Submit Version
        if "!spark-submit" in source:
            if "3.2.1" in source:
                new_submit = source.replace("3.2.1", "3.5.0")
                cell["source"] = [new_submit]
                print("Fixed spark-submit version in 04_sales_fraud.ipynb")

with open(nb_path, "w") as f:
    json.dump(nb, f, indent=1)
