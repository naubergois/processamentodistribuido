import json
import os

# --- Notebook Generation Logic ---

def create_notebook(filename, title, description, cells):
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"# {title}\n", "\n", f"{description}"]
            }
        ] + cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"codemirror_mode": {"name": "ipython", "version": 3}, "file_extension": ".py", "mimetype": "text/x-python", "name": "python", "nbconvert_exporter": "python", "pygments_lexer": "ipython3", "version": "3.10.12"}
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }
    
    os.makedirs("notebooks", exist_ok=True)
    with open(f"notebooks/{filename}", 'w') as f:
        json.dump(notebook, f, indent=1)
    print(f"Generated {filename}")

# --- Setup Blocks ---

def get_setup_env_block():
    return [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 1. Environment Setup\n\nThis cell installs **Java 8**, **Spark 3.5.0**, **Kafka 3.6.1**, and necessary Python libraries (`pyspark`, `kafka-python`, `redis`, `pymongo`, `elasticsearch`, `cassandra-driver`, `minio`). It also sets environment variables for Java and Spark."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Install Dependencies\n",
                "!apt-get install openjdk-8-jdk-headless -qq > /dev/null\n",
                "!wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz\n",
                "!tar xf spark-3.5.0-bin-hadoop3.tgz\n",
                "!wget -q https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz\n",
                "!tar xf kafka_2.13-3.6.1.tgz\n",
                "!pip install -q findspark pyspark kafka-python redis pymongo elasticsearch==7.10.1 cassandra-driver minio \"numpy<2.0.0\"\n",
                "\n",
                "# Environment Variables\n",
                "import os\n",
                "os.environ[\"JAVA_HOME\"] = \"/usr/lib/jvm/java-8-openjdk-amd64\"\n",
                "os.environ[\"SPARK_HOME\"] = \"/content/spark-3.5.0-bin-hadoop3\"\n",
                "import findspark\n",
                "findspark.init()"
            ]
        }
    ]

def get_service_start_block(services):
    code = []
    desc = ["## 2. Start Services\n\nThis cell starts the required distributed services in the background:"]
    
    if "kafka" in services:
        desc.append("- **Kafka & Zookeeper**: Event streaming platform.")
        code.extend([
            "# Start Kafka\n",
            "!!./kafka_2.13-3.6.1/bin/zookeeper-server-start.sh -daemon ./kafka_2.13-3.6.1/config/zookeeper.properties\n",
            "!!./kafka_2.13-3.6.1/bin/kafka-server-start.sh -daemon ./kafka_2.13-3.6.1/config/server.properties\n"
        ])
    if "redis" in services:
        desc.append("- **Redis**: In-memory data store.")
        code.extend([
            "# Start Redis\n",
            "!apt-get install redis-server -qq > /dev/null\n",
            "!service redis-server start\n"
        ])
    if "mongo" in services:
        desc.append("- **MongoDB**: NoSQL document database.")
        code.extend([
            "# Start MongoDB\n",
            "!wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add -\n",
            "!echo \"deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse\" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list\n",
            "!apt-get update -qq > /dev/null\n",
            "!apt-get install -y mongodb-org -qq > /dev/null\n",
            "!mkdir -p /data/db\n",
            "!mongod --fork --logpath /var/log/mongodb.log --bind_ip 127.0.0.1\n"
        ])
    if "es" in services:
        desc.append("- **Elasticsearch**: Search and analytics engine.")
        code.extend([
            "# Start Elasticsearch\n",
            "!wget -q https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-linux-x86_64.tar.gz\n",
            "!tar -xzf elasticsearch-7.10.2-linux-x86_64.tar.gz\n",
            "!chown -R daemon:daemon elasticsearch-7.10.2\n",
            "!!sudo -u daemon ES_JAVA_OPTS=\"-Xms512m -Xmx512m\" ./elasticsearch-7.10.2/bin/elasticsearch -d\n"
        ])
    if "cassandra" in services:
        desc.append("- **Cassandra**: Wide-column store.")
        code.extend([
            "# Start Cassandra\n",
            "!echo \"deb http://www.apache.org/dist/cassandra/debian 40x main\" | tee -a /etc/apt/sources.list.d/cassandra.sources.list\n",
            "!curl https://downloads.apache.org/cassandra/KEYS | apt-key add -\n",
            "!apt-get update -qq > /dev/null\n",
            "!apt-get install cassandra -qq > /dev/null\n",
            "!service cassandra start\n"
        ])
    if "minio" in services:
        desc.append("- **MinIO**: S3-compatible object storage.")
        code.extend([
            "# Start MinIO\n",
            "!wget -q https://dl.min.io/server/minio/release/linux-amd64/minio\n",
            "!chmod +x minio\n",
            "!./minio server /data --console-address \":9001\" &> minio.log &\n"
        ])
    
    code.append("\nimport time\ntime.sleep(30) # Wait for startup")
    return [
        {"cell_type": "markdown", "metadata": {}, "source": ["\n".join(desc)]},
        {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": code}
    ]

# --- Helper: Kafka Topic Creation ---
def get_topic_create_block():
    return [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 3. Create Kafka Topic\n\nCreates a topic named `input-topic` with 1 partition and replication factor 1."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "# Create Topic\n",
                "!./kafka_2.13-3.6.1/bin/kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1"
            ]
        }
    ]

def get_spark_submit_block(code_lines):
    return [
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": ["%%writefile kafka_consumer.py\n"] + code_lines
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": ["!spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 kafka_consumer.py"]
        }
    ]

# --- Notebook Defines (Renumbered 51-60) ---

# 51: Basics
def get_nb51():
    setup = get_service_start_block(["kafka"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nThis cell runs a Python Kafka Producer that sends 100 simple text messages to `input-topic`."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import time\n",
                "print(\"Starting Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 messages...\")\n",
                "for i in range(100):\n",
                "    producer.send('input-topic', f'message_{i}'.encode('utf-8'))\n",
                "    time.sleep(0.05)\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark Streaming Consumer\n\nThis cell initializes Spark, reads from Kafka, and prints the messages to the console."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import time\n",
        "print(\"Initializing Spark Session...\")\n",
        "spark = SparkSession.builder.appName(\"Basics\").getOrCreate()\n",
        "print(\"Reading from Kafka...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "# Write to JSON for Verification\n",
        "print(\"Writing output to JSON...\")\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.format(\"json\").option(\"path\", \"/content/output_nb51\").option(\"checkpointLocation\", \"/content/checkpoint_nb51\").start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck for generated output files to confirm processing success."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import os\n",
                "import glob\n",
                "\n",
                "print(\"Checking output directory...\")\n",
                "if os.path.exists('/content/output_nb51'):\n",
                "    files = glob.glob('/content/output_nb51/*.json')\n",
                "    print(f\"Found {len(files)} JSON output files.\")\n",
                "    if files:\n",
                "        print(\"--- Sample Content ---\")\n",
                "        with open(files[0], 'r') as f: print(f.read())\n",
                "    else:\n",
                "        print(\"No JSON files found yet. (Job might still be starting)\")\n",
                "else:\n",
                "    print(\"Output directory not found.\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 52: Cassandra
def get_nb52():
    setup = get_service_start_block(["kafka", "cassandra"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSimulates sensor data (`id`, `temp`) and sends JSON messages to Kafka."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Sensor Data Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))\n",
                "print(\"Sending 100 sensor readings...\")\n",
                "for _ in range(100):\n",
                "    data = {'id': f's{random.randint(1,5)}', 'temp': random.uniform(20.0, 30.0)}\n",
                "    producer.send('input-topic', data)\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark -> Cassandra\n\nInitialize Cassandra keyspace/table and use Spark `foreachBatch` to insert data."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from cassandra.cluster import Cluster\n",
        "import json\n",
        "\n",
        "# Init Cassandra Schema\n",
        "cluster = Cluster(['127.0.0.1'])\n",
        "session = cluster.connect()\n",
        "session.execute(\"CREATE KEYSPACE IF NOT EXISTS sensors WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}\")\n",
        "session.execute(\"CREATE TABLE IF NOT EXISTS sensors.data (id text PRIMARY KEY, temp float)\")\n",
        "session.shutdown()\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Cassandra\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    cluster_local = Cluster(['127.0.0.1'])\n",
        "    session_local = cluster_local.connect('sensors')\n",
        "    for row in rows:\n",
        "        val = json.loads(row.value)\n",
        "        session_local.execute(f\"INSERT INTO data (id, temp) VALUES ('{val['id']}', {val['temp']})\")\n",
        "    session_local.shutdown()\n",
        "    print(f\"Batch {epoch_id} persisted.\")\n",
        "\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nQuery Cassandra to verify data storage."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from cassandra.cluster import Cluster\n",
                "cluster = Cluster(['127.0.0.1'])\n",
                "session = cluster.connect('sensors')\n",
                "rows = session.execute(\"SELECT * FROM data LIMIT 10\")\n",
                "print(\"--- Data in Cassandra ---\")\n",
                "for row in rows: print(row)\n",
                "session.shutdown()"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 53: Elastic
def get_nb53():
    setup = get_service_start_block(["kafka", "es"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nGenerates log messages (`INFO` or `ERROR`) and sends them to Kafka."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Log Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 log messages...\")\n",
                "for _ in range(100):\n",
                "    log = {'timestamp': time.time(), 'level': random.choice(['INFO', 'ERROR'])}\n",
                "    producer.send('input-topic', json.dumps(log).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark -> Elasticsearch\n\nReads logs from Kafka and indexes them into Elasticsearch index `logs`."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from elasticsearch import Elasticsearch\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Elastic\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    es = Elasticsearch(['http://localhost:9200'])\n",
        "    for row in rows:\n",
        "        doc = json.loads(row.value)\n",
        "        es.index(index='logs', body=doc)\n",
        "    print(f\"Batch {epoch_id} processed: {len(rows)} logs indexed.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nSearch Elasticsearch to verify indexed logs."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from elasticsearch import Elasticsearch\n",
                "es = Elasticsearch(['http://localhost:9200'])\n",
                "time.sleep(2) # Wait for flush\n",
                "print(\"Querying Elasticsearch...\")\n",
                "res = es.search(index=\"logs\", body={\"query\": {\"match_all\": {}}, \"size\": 5})\n",
                "print(\"--- Logs in Elasticsearch ---\")\n",
                "for hit in res['hits']['hits']:\n",
                "    print(hit['_source'])"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 54: MinIO
def get_nb54():
    setup = get_service_start_block(["kafka", "minio"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends file data chunks to Kafka."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import time\n",
                "print(\"Starting Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 data chunks...\")\n",
                "for i in range(100): producer.send('input-topic', f'data_{i}'.encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark -> MinIO (Data Lake)\n\nAggregates messages in a batch and uploads them as a file to MinIO bucket `spark-bucket`."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from minio import Minio\n",
        "import io\n",
        "\n",
        "m_client = Minio(\"127.0.0.1:9000\", access_key=\"minioadmin\", secret_key=\"minioadmin\", secure=False)\n",
        "if not m_client.bucket_exists(\"spark-bucket\"): m_client.make_bucket(\"spark-bucket\")\n",
        "\n",
        "spark = SparkSession.builder.appName(\"MinIO\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    val = \"\\n\".join([r.value.decode('utf-8') for r in df.collect()])\n",
        "    if val:\n",
        "        m_client.put_object(\"spark-bucket\", f\"batch_{epoch_id}.txt\", io.BytesIO(val.encode('utf-8')), len(val))\n",
        "    print(f\"Batch {epoch_id} uploaded to MinIO.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nList objects in MinIO bucket."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from minio import Minio\n",
                "m_client = Minio(\"127.0.0.1:9000\", access_key=\"minioadmin\", secret_key=\"minioadmin\", secure=False)\n",
                "print(\"Listing objects in MinIO bucket...\")\n",
                "objects = m_client.list_objects(\"spark-bucket\")\n",
                "print(\"--- Files in MinIO ---\")\n",
                "for obj in objects:\n",
                "    print(obj.object_name)"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 55: Redis
def get_nb55():
    setup = get_service_start_block(["kafka", "redis"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends User IDs to Kafka. Also pre-loads User Names into Redis."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import time, redis\n",
                "\n",
                "# Preload Redis\n",
                "print(\"Preloading Redis...\")\n",
                "r = redis.Redis()\n",
                "for i in range(1, 11): r.set(f\"user:{i}\", f\"User_{i}\")\n",
                "\n",
                "print(\"Starting Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "for i in range(1, 10): producer.send('input-topic', f'{i}'.encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark Enrichment with Redis\n\nFor each message (User ID), look up the name in Redis and print the Enriched result."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Redis\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r_local = redis.Redis()\n",
        "    for row in rows:\n",
        "         uid = row.value.decode('utf-8')\n",
        "         name = r_local.get(f\"user:{uid}\")\n",
        "         if name: print(f\"Enriched: {uid} -> {name.decode('utf-8')}\")\n",
        "\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(20)"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Redis keys."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "r = redis.Redis()\n",
                "keys = r.keys(\"user:*\")\n",
                "print(f\"--- Found {len(keys)} users in Redis ---\")\n",
                "print(keys[:5])"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 56: Mongo
def get_nb56():
    setup = get_service_start_block(["kafka", "mongo"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends JSON objects to Kafka."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time\n",
                "print(\"Starting JSON Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 50 records to Kafka...\")\n",
                "for i in range(50): producer.send('input-topic', json.dumps({'id': i}).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark -> MongoDB\n\nWrites processed objects into MongoDB collection `ecommerce.orders`."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from pymongo import MongoClient\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Mongo\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    mongo = MongoClient()\n",
        "    for row in rows:\n",
        "        doc = json.loads(row.value)\n",
        "        mongo.ecommerce.orders.insert_one(doc)\n",
        "    print(f\"Batch {epoch_id} inserted into MongoDB.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(20)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCount documents in MongoDB."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from pymongo import MongoClient\n",
                "m = MongoClient()\n",
                "count = m.ecommerce.orders.count_documents({})\n",
                "print(f\"--- Total Documents in MongoDB: {count} ---\")\n",
                "print(m.ecommerce.orders.find_one())"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 57: Hot/Cold
def get_nb57():
    setup = get_service_start_block(["kafka", "es", "minio"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends data tagged as 'hot' or 'cold'."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Hot/Cold Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 items...\")\n",
                "for i in range(100):\n",
                "    data = {'type': random.choice(['hot', 'cold']), 'val': i}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark Hot/Cold Routing\n\nRoutes 'hot' data to Elasticsearch and 'cold' data to MinIO."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from elasticsearch import Elasticsearch\n",
        "from minio import Minio\n",
        "import json, io\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Router\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    es = Elasticsearch(['http://localhost:9200'])\n",
        "    m = Minio(\"127.0.0.1:9000\", access_key=\"minioadmin\", secret_key=\"minioadmin\", secure=False)\n",
        "    if not m.bucket_exists(\"cold\"): m.make_bucket(\"cold\")\n",
        "    for row in rows:\n",
        "        d = json.loads(row.value)\n",
        "        if d['type'] == 'hot':\n",
        "            es.index(index='hot_data', body=d)\n",
        "        else:\n",
        "            m.put_object(\"cold\", f\"obj_{d['val']}\", io.BytesIO(row.value), len(row.value))\n",
        "    print(f\"Batch {epoch_id} routed.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck both destinations."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from elasticsearch import Elasticsearch\n",
                "from minio import Minio\n",
                "\n",
                "# Check ES\n",
                "es = Elasticsearch(['http://localhost:9200'])\n",
                "time.sleep(2)\n",
                "res = es.count(index=\"hot_data\")\n",
                "print(f\"Hot Data (ES): {res['count']} docs\")\n",
                "\n",
                "# Check MinIO\n",
                "m = Minio(\"127.0.0.1:9000\", access_key=\"minioadmin\", secret_key=\"minioadmin\", secure=False)\n",
                "objs = list(m.list_objects(\"cold\"))\n",
                "print(f\"Cold Data (MinIO): {len(objs)} files\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 58: Fraud Redis
def get_nb58():
    setup = get_service_start_block(["kafka", "redis"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSimulates a user jumping locations rapidly."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "producer.send('input-topic', json.dumps({'user': 'u1', 'loc': 'NY'}).encode('utf-8')) # Init state\n",
                "time.sleep(1)\n",
                "producer.send('input-topic', json.dumps({'user': 'u1', 'loc': 'CA'}).encode('utf-8')) # Fraud\n",
                "producer.flush()"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark Fraud Detection\n\nCompares current location with the last known location stored in Redis."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis, json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Fraud\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r = redis.Redis()\n",
        "    for row in rows:\n",
        "        d = json.loads(row.value)\n",
        "        last = r.get(f\"loc:{d['user']}\")\n",
        "        if last and last.decode('utf-8') != d['loc']:\n",
        "             print(f\"FRAUD ALERT: User {d['user']} jump {last.decode('utf-8')} -> {d['loc']}\")\n",
        "        r.set(f\"loc:{d['user']}\", d['loc'])\n",
        "    \n",
        "print(\"Starting Spark Fraud Detector...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(20)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck current state in Redis."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "r = redis.Redis()\n",
                "print(f\"Current Location of u1: {r.get('loc:u1')}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 59: IoT Cassandra
def get_nb59():
    setup = get_service_start_block(["kafka", "cassandra"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends sensor events."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting IoT Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 events...\")\n",
                "for i in range(500):\n",
                "    producer.send('input-topic', json.dumps({'id': 's1', 'val': i}).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark Aggregation -> Cassandra\n\nCounts events in the batch and updates a counter in Cassandra."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from cassandra.cluster import Cluster\n",
        "\n",
        "# Init Keyspace\n",
        "cluster = Cluster(['127.0.0.1'])\n",
        "session = cluster.connect()\n",
        "session.execute(\"CREATE KEYSPACE IF NOT EXISTS iot WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}\")\n",
        "session.execute(\"CREATE TABLE IF NOT EXISTS iot.aggs (id text PRIMARY KEY, count int)\")\n",
        "session.shutdown()\n",
        "\n",
        "spark = SparkSession.builder.appName(\"IoT\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    count = df.count()\n",
        "    if count > 0:\n",
        "        cluster = Cluster(['127.0.0.1'])\n",
        "        session = cluster.connect('iot')\n",
        "        session.execute(f\"INSERT INTO aggs (id, count) VALUES ('s1', {count})\")\n",
        "        session.shutdown()\n",
        "        print(f\"Batch {epoch_id}: Updates sent to Cassandra.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck accumulated count in Cassandra."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from cassandra.cluster import Cluster\n",
                "cluster = Cluster(['127.0.0.1'])\n",
                "session = cluster.connect('iot')\n",
                "print(session.execute(\"SELECT * FROM aggs\").one())"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 60: Full Pipeline
def get_nb60():
    setup = get_service_start_block(["kafka", "mongo"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends mix of valid and corrupt data."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Mixed Data Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 records...\")\n",
                "for _ in range(100):\n",
                "    if random.random() < 0.2: data = {'status': 'corrupt'}\n",
                "    else: data = {'status': 'valid', 'payload': 'ok'}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Spark Quality Check -> Mongo/DLQ\n\nValid records go to MongoDB `pipeline.valid`, invalid ones to `pipeline.dlq`."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from pymongo import MongoClient\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Pipeline\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    m = MongoClient()\n",
        "    for row in rows:\n",
        "        d = json.loads(row.value)\n",
        "        if d['status'] == 'valid':\n",
        "             m.pipeline.valid.insert_one(d)\n",
        "        else:\n",
        "             m.pipeline.dlq.insert_one(d)\n",
        "             print(\"Sent to DLQ\")\n",
        "    print(f\"Batch {epoch_id} pipeline processed\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck counts in both collections."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from pymongo import MongoClient\n",
                "m = MongoClient()\n",
                "print(f\"Valid: {m.pipeline.valid.count_documents({})}\")\n",
                "print(f\"DLQ: {m.pipeline.dlq.count_documents({})}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# --- Main execution ---
if __name__ == "__main__":
    create_notebook("51_colab_kafka_spark_basics.ipynb", "NB51: Kafka & Spark Basics", "Introduction to distributed streaming (Kafka + Spark) in Colab.", get_nb51())
    create_notebook("52_colab_spark_cassandra.ipynb", "NB52: Spark + Cassandra", "Persisting streaming sensor data to Cassandra.", get_nb52())
    create_notebook("53_colab_spark_elasticsearch.ipynb", "NB53: Spark + Elasticsearch", "Real-time log analysis indexing to Elasticsearch.", get_nb53())
    create_notebook("54_colab_spark_minio.ipynb", "NB54: Spark + MinIO", "Writing streaming data to a MinIO Data Lake.", get_nb54())
    create_notebook("55_colab_spark_redis_enrich.ipynb", "NB55: Spark + Redis Enrichment", "Low-latency stream enrichment using Redis lookups.", get_nb55())
    create_notebook("56_colab_spark_mongo_sink.ipynb", "NB56: Spark + MongoDB", "Storing E-commerce orders in MongoDB.", get_nb56())
    create_notebook("57_colab_hot_cold_arch.ipynb", "NB57: Hot/Cold Architecture", "Routing Hot data to ES and Cold data to MinIO.", get_nb57())
    create_notebook("58_colab_fraud_redis.ipynb", "NB58: Real-time Fraud Detection", "Stateful fraud detection using Redis for state tracking.", get_nb58())
    create_notebook("59_colab_iot_cassandra.ipynb", "NB59: IoT Aggregation", "Windowed aggregation of IoT metrics stored in Cassandra.", get_nb59())
    create_notebook("60_colab_full_pipeline.ipynb", "NB60: Complete Data Pipeline", "End-to-end pipeline with data validation and DLQ.", get_nb60())
