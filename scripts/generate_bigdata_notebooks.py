import json
import os

def create_notebook(filename, title, description, code_cells):
    # Common Setup Cell for Big Data Stack
    setup_code = [
        "# 1. Install Java 17 (Required for Trino)\n",
        "!apt-get install openjdk-17-jdk-headless -qq > /dev/null\n",
        "import os\n",
        "os.environ[\"JAVA_HOME\"] = \"/usr/lib/jvm/java-17-openjdk-amd64\"\n",
        "\n",
        "# 2. Install PySpark & Dependencies\n",
        "!pip install -q pyspark==3.5.0 elasticsearch==8.11.0\n",
        "\n",
        "# 3. Download & Start Elasticsearch 7.17 (Background)\n",
        "%%bash\n",
        "if [ ! -d \"elasticsearch-7.17.9\" ]; then\n",
        "  wget -q https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.9-linux-x86_64.tar.gz\n",
        "  tar -xzf elasticsearch-7.17.9-linux-x86_64.tar.gz\n",
        "  chown -R daemon:daemon elasticsearch-7.17.9\n",
        "fi\n",
        "# Start as daemon\n",
        "sudo -u daemon ./elasticsearch-7.17.9/bin/elasticsearch -d -E discovery.type=single-node -E http.port=9200 -E xpack.security.enabled=false\n",
        "\n",
        "# 4. Download & Start Trino (Presto) Server\n",
        "if [ ! -d \"trino-server-422\" ]; then\n",
        "  wget -q https://repo1.maven.org/maven2/io/trino/trino-server/422/trino-server-422.tar.gz\n",
        "  tar -xzf trino-server-422.tar.gz\n",
        "  # Config\n",
        "  mkdir -p trino-server-422/etc/catalog\n",
        "  echo 'coordinator=true' > trino-server-422/etc/node.properties\n",
        "  echo 'node-scheduler.include-coordinator=true' >> trino-server-422/etc/node.properties\n",
        "  echo 'http-server.http.port=8080' >> trino-server-422/etc/node.properties\n",
        "  echo 'query.max-memory=5GB' >> trino-server-422/etc/node.properties\n",
        "  echo 'discovery.uri=http://127.0.0.1:8080' > trino-server-422/etc/config.properties\n",
        "  # JVM Config\n",
        "  echo '-server' > trino-server-422/etc/jvm.config\n",
        "  echo '-Xmx2G' >> trino-server-422/etc/jvm.config\n",
        "  # Elasticsearch Catalog\n",
        "  echo 'connector.name=elasticsearch' > trino-server-422/etc/catalog/es.properties\n",
        "  echo 'elasticsearch.host=localhost' >> trino-server-422/etc/catalog/es.properties\n",
        "  echo 'elasticsearch.port=9200' >> trino-server-422/etc/catalog/es.properties\n",
        "  echo 'elasticsearch.default-schema-name=default' >> trino-server-422/etc/catalog/es.properties\n",
        "  # TPCH Catalog\n",
        "  echo 'connector.name=tpch' > trino-server-422/etc/catalog/tpch.properties\n",
        "fi\n",
        "# Start Trino\n",
        "./trino-server-422/bin/launcher start\n",
        "\n",
        "# 5. Install Trino CLI\n",
        "if [ ! -f \"trino\" ]; then\n",
        "  wget -q https://repo1.maven.org/maven2/io/trino/trino-cli/422/trino-cli-422-executable.jar -O trino\n",
        "  chmod +x trino\n",
        "fi\n",
        "\n",
        "print(\"Environment Setup Complete. Waiting for services to startup...\")\n",
        "import time\n",
        "time.sleep(30) # Wait for ES and Trino"
    ]

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
                "source": ["## 1. Environment Setup (Spark, ES, Trino)"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": setup_code
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


# --- 36. Log Analytics ---
nb36_code = [
    [
        "from pyspark.sql import SparkSession\n",
        "from pyspark.sql.functions import current_timestamp, lit, expr\n",
        "import time, random\n",
        "\n",
        "# 1. Initialize Spark with ES Connector\n",
        "# Note: In Colab, we need to download the jar manually or use --packages if we were using spark-submit.\n",
        "# Here we use pyspark shell method. For simplicity, we assume the connector jar is available or we download it.\n",
        "!wget -q https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar\n",
        "\n",
        "spark = SparkSession.builder \\\n",
        "    .appName(\"LogAnalytics\") \\\n",
        "    .config(\"spark.driver.extraClassPath\", \"elasticsearch-spark-30_2.12-8.11.0.jar\") \\\n",
        "    .master(\"local[*]\") \\\n",
        "    .getOrCreate()\n",
        "\n",
        "# 2. Generate Log Data\n",
        "logs = []\n",
        "levels = [\"INFO\", \"WARN\", \"ERROR\"]\n",
        "services = [\"auth\", \"payment\", \"search\"]\n",
        "for i in range(100):\n",
        "    logs.append({\n",
        "        \"id\": str(i),\n",
        "        \"level\": random.choice(levels),\n",
        "        \"service\": random.choice(services),\n",
        "        \"message\": f\"Event {i} occurred\",\n",
        "        \"timestamp\": random.randint(1600000000, 1700000000)\n",
        "    })\n",
        "\n",
        "df = spark.createDataFrame(logs)\n",
        "df.show(5)\n",
        "\n",
        "# 3. Write to Elasticsearch\n",
        "df.write \\\n",
        "    .format(\"org.elasticsearch.spark.sql\") \\\n",
        "    .option(\"es.nodes\", \"localhost\") \\\n",
        "    .option(\"es.port\", \"9200\") \\\n",
        "    .option(\"es.resource\", \"logs/server_logs\") \\\n",
        "    .option(\"es.nodes.wan.only\", \"true\") \\\n",
        "    .mode(\"overwrite\") \\\n",
        "    .save()\n",
        "\n",
        "print(\"Data written to Elasticsearch index 'logs'.\")"
    ],
    [
        "# 4. Query with Trino\n",
        "print(\"Querying with Trino...\")\n",
        "!./trino --server localhost:8080 --catalog es --schema default --execute \"SELECT level, count(*) as count FROM logs GROUP BY level\""
    ]
]

# --- 37. Product Catalog ---
nb37_code = [
    [
        "from pyspark.sql import SparkSession\n",
        "import random\n",
        "\n",
        "!wget -q -nc https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar\n",
        "spark = SparkSession.builder.config(\"spark.driver.extraClassPath\", \"elasticsearch-spark-30_2.12-8.11.0.jar\").getOrCreate()\n",
        "\n",
        "products = []\n",
        "categories = [\"Electronics\", \"Clothing\", \"Home\"]\n",
        "for i in range(50):\n",
        "    products.append({\n",
        "        \"sku\": f\"SKU-{i}\",\n",
        "        \"name\": f\"Product {i}\",\n",
        "        \"category\": random.choice(categories),\n",
        "        \"price\": float(random.randint(10, 500))\n",
        "    })\n",
        "\n",
        "df = spark.createDataFrame(products)\n",
        "df.write.format(\"org.elasticsearch.spark.sql\").option(\"es.nodes\",\"localhost\").option(\"es.resource\",\"products/catalog\").mode(\"overwrite\").save()\n",
        "print(\"Catalog indexed.\")"
    ],
    [
        "# Query expensive electronics\n",
        "!./trino --server localhost:8080 --catalog es --schema default --execute \"SELECT name, price FROM products WHERE category='Electronics' AND price > 200\""
    ]
]

# --- 38. SIEM ---
nb38_code = [
    [
        "# Using Micro-batch streaming to ES\n",
        "from pyspark.sql import SparkSession\n",
        "from pyspark.sql.functions import *\n",
        "\n",
        "!wget -q -nc https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar\n",
        "spark = SparkSession.builder.config(\"spark.driver.extraClassPath\", \"elasticsearch-spark-30_2.12-8.11.0.jar\").getOrCreate()\n",
        "\n",
        "# Simulating stream using Rate source\n",
        "stream_df = spark.readStream.format(\"rate\").option(\"rowsPerSecond\", 5).load()\n",
        "\n",
        "events = stream_df.select(\n",
        "    col(\"timestamp\"),\n",
        "    expr(\"case when value % 10 = 0 then 'MALICIOUS' else 'NORMAL' end\").alias(\"event_type\"),\n",
        "    lit(\"192.168.1.1\").alias(\"src_ip\"),\n",
        "    col(\"value\").alias(\"id\")\n",
        ")\n",
        "\n",
        "query = events.writeStream \\\n",
        "    .format(\"org.elasticsearch.spark.sql\") \\\n",
        "    .option(\"checkpointLocation\", \"/tmp/chk_siem\") \\\n",
        "    .option(\"es.resource\", \"siem/events\") \\\n",
        "    .option(\"es.nodes\", \"localhost\") \\\n",
        "    .start()\n",
        "\n",
        "query.awaitTermination(30)"
    ],
    [
        "print(\"Hunting for threats with Trino...\")\n",
        "!./trino --server localhost:8080 --catalog es --schema default --execute \"SELECT * FROM siem WHERE event_type='MALICIOUS' LIMIT 5\""
    ]
]

# --- 39. Customer 360 ---
nb39_code = [
    [
        "from pyspark.sql import SparkSession\n",
        "import random\n",
        "\n",
        "!wget -q -nc https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar\n",
        "spark = SparkSession.builder.config(\"spark.driver.extraClassPath\", \"elasticsearch-spark-30_2.12-8.11.0.jar\").getOrCreate()\n",
        "\n",
        "users = [{\"id\": i, \"spend\": random.randint(100, 1000), \"visits\": random.randint(1,50)} for i in range(20)]\n",
        "df = spark.createDataFrame(users)\n",
        "\n",
        "# Aggregate/Enrich\n",
        "df_agg = df.withColumn(\"segment\", expr(\"case when spend > 500 then 'VIP' else 'Regular' end\"))\n",
        "df_agg.write.format(\"org.elasticsearch.spark.sql\").option(\"es.nodes\",\"localhost\").option(\"es.resource\",\"customers/profile\").mode(\"overwrite\").save()\n",
        "print(\"Customer profiles synced.\")"
    ],
    [
        "# Analytics on VIPs\n",
        "!./trino --server localhost:8080 --catalog es --schema default --execute \"SELECT segment, avg(spend) as avg_spend FROM customers GROUP BY segment\""
    ]
]

# --- 40. Hybrid Analytics ---
nb40_code = [
    [
        "from pyspark.sql import SparkSession\n",
        "\n",
        "spark = SparkSession.builder.getOrCreate()\n",
        "\n",
        "# Create local sales data\n",
        "sales = [{\"sale_id\": i, \"customer_id\": i % 5, \"amount\": 100} for i in range(20)]\n",
        "spark.createDataFrame(sales).write.format(\"org.elasticsearch.spark.sql\").option(\"es.nodes\",\"localhost\").option(\"es.resource\",\"sales/trx\").mode(\"overwrite\").save()\n",
        "\n",
        "print(\"Sales data in ES. Now let's join with TPCH (Orders) in Trino.\")"
    ],
    [
        "# Hybrid Query: Join ES data with TPCH data generated on the fly by Trino\n",
        "sql = \"\"\"\n",
        "SELECT \n",
        "  s.sale_id, \n",
        "  s.amount, \n",
        "  o.orderstatus \n",
        "FROM \n",
        "  es.default.sales s, \n",
        "  tpch.tiny.orders o \n",
        "WHERE \n",
        "  s.sale_id = o.orderkey \n",
        "LIMIT 5\n",
        "\"\"\"\n",
        "print(f\"Executing: {sql}\")\n",
        "!./trino --server localhost:8080 --execute \"{sql}\""
    ]
]


create_notebook("36_log_analytics.ipynb", "Example 36: Log Analytics", "Spark ingest, Elastic Storage, Presto Query.", nb36_code)
create_notebook("37_product_catalog.ipynb", "Example 37: Product Search", "Indexing catalog for faceted search.", nb37_code)
create_notebook("38_siem_hunting.ipynb", "Example 38: SIEM Threat Hunting", "Real-time security event monitoring.", nb38_code)
create_notebook("39_customer_360.ipynb", "Example 39: Customer 360", "Aggregating customer profiles.", nb39_code)
create_notebook("40_hybrid_analytics.ipynb", "Example 40: Hybrid Analytics", "Presto Federation: ES + TPCH.", nb40_code)
