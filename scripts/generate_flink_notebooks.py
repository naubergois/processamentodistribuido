import json
import os

# Notebook Template
def create_notebook(filename, title, description, code_cells):
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"# {title}\n", "\n", f"{description}"]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 1. Environment Setup"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Install Java 11\n",
                    "!apt-get install openjdk-11-jdk-headless -qq > /dev/null\n",
                    "import os\n",
                    "os.environ[\"JAVA_HOME\"] = \"/usr/lib/jvm/java-11-openjdk-amd64\"\n",
                    "\n",
                    "# Install Apache Flink\n",
                    "!pip install -q apache-flink"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"codemirror_mode": {"name": "ipython", "version": 3}, "file_extension": ".py", "mimetype": "text/x-python", "name": "python", "nbconvert_exporter": "python", "pygments_lexer": "ipython3", "version": "3.10.12"}
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    # Append custom logic cells
    for cell_source in code_cells:
        notebook["cells"].append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": cell_source
        })

    with open(f"notebooks/{filename}", 'w') as f:
        json.dump(notebook, f, indent=1)
    print(f"Generated {filename}")

# --- Logic Definitions ---

# 31: WordCount
nb31_code = [
    [
        "from pyflink.datastream import StreamExecutionEnvironment\n",
        "\n",
        "env = StreamExecutionEnvironment.get_execution_environment()\n",
        "env.set_parallelism(1)\n",
        "\n",
        "data = [\"hello flink\", \"streaming is cool\", \"hello cloud\", \"flink is fast\"]\n",
        "ds = env.from_collection(data)\n",
        "\n",
        "def split(line):\n",
        "    return line.split(\" \")\n",
        "\n",
        "ds.flat_map(split) \\\n",
        "  .map(lambda i: (i, 1), output_type=None) \\\n",
        "  .key_by(lambda i: i[0]) \\\n",
        "  .sum(1) \\\n",
        "  .print()\n",
        "\n",
        "env.execute(\"wordcount\")"
    ]
]

# 32: Fraud Detection
nb32_code = [
    [
        "from pyflink.datastream import StreamExecutionEnvironment\n",
        "\n",
        "env = StreamExecutionEnvironment.get_execution_environment()\n",
        "env.set_parallelism(1)\n",
        "\n",
        "transactions = [\n",
        "    {\"id\": 1, \"amount\": 50.0, \"country\": \"US\"},\n",
        "    {\"id\": 2, \"amount\": 12000.0, \"country\": \"US\"},  # Fraud\n",
        "    {\"id\": 3, \"amount\": 20.0, \"country\": \"BR\"},\n",
        "    {\"id\": 4, \"amount\": 15000.0, \"country\": \"RU\"},  # Fraud\n",
        "]\n",
        "\n",
        "ds = env.from_collection(transactions)\n",
        "\n",
        "# Filter > 10000\n",
        "ds.filter(lambda tx: tx[\"amount\"] > 10000) \\\n",
        "  .map(lambda tx: f\"ALERT: High value transaction {tx['id']} detected: ${tx['amount']}\") \\\n",
        "  .print()\n",
        "\n",
        "env.execute(\"fraud-check\")"
    ]
]

# 33: IoT Windows
nb33_code = [
    [
        "from pyflink.common import Types\n",
        "from pyflink.datastream import StreamExecutionEnvironment\n",
        "from pyflink.datastream.window import TumblingProcessingTimeWindows\n",
        "from pyflink.common.time import Time\n",
        "import time\n",
        "\n",
        "env = StreamExecutionEnvironment.get_execution_environment()\n",
        "env.set_parallelism(1)\n",
        "\n",
        "# Simulating continuous stream\n",
        "# For simplicity here, we stick to collection but Flink windows usually need time\n",
        "# In Colab 'processing time' is easiest to demo without event-time complexity\n",
        "\n",
        "# (SensorID, Temp)\n",
        "data = [\n",
        "    (\"s1\", 20), (\"s1\", 22), (\"s2\", 15), (\"s1\", 25), \n",
        "    (\"s2\", 18), (\"s1\", 23), (\"s2\", 16), (\"s1\", 24)\n",
        "]\n",
        "\n",
        "ds = env.from_collection(data)\n",
        "\n",
        "ds.key_by(lambda x: x[0]) \\\n",
        "  .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5))) \\n",
        "  .reduce(lambda a, b: (a[0], (a[1] + b[1]) / 2)) \\n",
        "  .print()\n",
        "\n",
        "# Note: With from_collection, processing time windows might trigger fast.\n",
        "# In real streaming, we would use a SourceFunction.\n",
        "env.execute(\"iot-windows\")"
    ]
]

# 34: Enrichment
nb34_code = [
    [
        "from pyflink.datastream import StreamExecutionEnvironment\n",
        "\n",
        "env = StreamExecutionEnvironment.get_execution_environment()\n",
        "env.set_parallelism(1)\n",
        "\n",
        "# Static User DB (Simulated)\n",
        "user_db = {\n",
        "    101: {\"name\": \"Alice\", \"risk\": \"low\"},\n",
        "    102: {\"name\": \"Bob\", \"risk\": \"high\"}\n",
        "}\n",
        "\n",
        "# Stream of click events\n",
        "clicks = [\n",
        "    {\"user_id\": 101, \"url\": \"/home\"},\n",
        "    {\"user_id\": 102, \"url\": \"/pay\"},\n",
        "    {\"user_id\": 101, \"url\": \"/products\"}\n",
        "]\n",
        "\n",
        "def enrich(event):\n",
        "    uid = event[\"user_id\"]\n",
        "    info = user_db.get(uid, {\"name\": \"Unknown\", \"risk\": \"unknown\"})\n",
        "    event.update(info)\n",
        "    return event\n",
        "\n",
        "ds = env.from_collection(clicks)\n",
        "ds.map(enrich).print()\n",
        "\n",
        "env.execute(\"enrichment\")"
    ]
]

# 35: Flink SQL
nb35_code = [
    [
        "from pyflink.table import EnvironmentSettings, TableEnvironment\n",
        "\n",
        "# Create Table Environment\n",
        "env_settings = EnvironmentSettings.in_streaming_mode()\n",
        "t_env = TableEnvironment.create(env_settings)\n",
        "\n",
        "# Define Source Table (Datagen)\n",
        "t_env.execute_sql(\"\"\"\n",
        "    CREATE TEMPORARY TABLE source_table (\n",
        "        id INT,\n",
        "        val INT\n",
        "    ) WITH (\n",
        "        'connector' = 'datagen',\n",
        "        'rows-per-second' = '5',\n",
        "        'fields.id.kind' = 'sequence',\n",
        "        'fields.id.start' = '1',\n",
        "        'fields.id.end' = '20',\n",
        "        'fields.val.min' = '1',\n",
        "        'fields.val.max' = '100'\n",
        "    )\n",
        "\"\"\")\n",
        "\n",
        "# Define Sink Table (Print)\n",
        "t_env.execute_sql(\"\"\"\n",
        "    CREATE TEMPORARY TABLE print_sink (\n",
        "        id INT,\n",
        "        val INT,\n",
        "        doubled_val INT\n",
        "    ) WITH (\n",
        "        'connector' = 'print'\n",
        "    )\n",
        "\"\"\")\n",
        "\n",
        "# Process & Insert\n",
        "t_env.execute_sql(\"\"\"\n",
        "    INSERT INTO print_sink\n",
        "    SELECT id, val, val * 2 FROM source_table\n",
        "\"\"\").wait()"
    ]
]


# Generate
create_notebook("31_flink_wordcount.ipynb", "Example 31: Flink WordCount", "Basic Streaming WordCount.", nb31_code)
create_notebook("32_flink_fraud_detection.ipynb", "Example 32: Fraud Detection", "Filtering transactions > $10k.", nb32_code)
create_notebook("33_flink_iot_windows.ipynb", "Example 33: IoT Windows", "Tumbling Windows aggregation.", nb33_code)
create_notebook("34_flink_enrichment.ipynb", "Example 34: Data Enrichment", "Joining stream with static lookup.", nb34_code)
create_notebook("35_flink_sql_dashboard.ipynb", "Example 35: Flink SQL", "Declarative processing with SQL.", nb35_code)
