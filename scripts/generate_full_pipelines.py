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
            "source": ["## 1. Environment Setup\n\nInstalls **Java 8**, **Spark 3.5.0**, **Kafka 3.6.1**, and Python libraries (PySpark, Kafka-Python, Redis, Mongo, ES, Cassandra, MinIO)."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Install Dependencies (Java 8, Spark 3.5.0, Kafka 3.6.1)\n",
                "!apt-get install openjdk-8-jdk-headless -qq > /dev/null\n",
                "!wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz\n",
                "!tar xf spark-3.5.0-bin-hadoop3.tgz\n",
                "!wget -q https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz\n",
                "!tar xf kafka_2.13-3.6.1.tgz\n",
                "!pip install -q \"numpy<2.0.0\" findspark pyspark kafka-python redis pymongo elasticsearch==7.10.1 cassandra-driver minio\n",
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
    desc = ["## 2. Start Services\n\nStarts background services needed for this pipeline:"]
    
    if "kafka" in services:
        desc.append("- **Kafka** (Zookeeper + Broker)")
        code.extend([
            "# Start Kafka\n",
            "!./kafka_2.13-3.6.1/bin/zookeeper-server-start.sh -daemon ./kafka_2.13-3.6.1/config/zookeeper.properties\n",
            "!./kafka_2.13-3.6.1/bin/kafka-server-start.sh -daemon ./kafka_2.13-3.6.1/config/server.properties\n"
        ])
    if "redis" in services:
        desc.append("- **Redis**")
        code.extend([
            "# Start Redis\n",
            "!apt-get install redis-server -qq > /dev/null\n",
            "!service redis-server start\n"
        ])
    if "mongo" in services:
        desc.append("- **MongoDB**")
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
        desc.append("- **Elasticsearch**")
        code.extend([
            "# Start Elasticsearch\n",
            "!wget -q https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-linux-x86_64.tar.gz\n",
            "!tar -xzf elasticsearch-7.10.2-linux-x86_64.tar.gz\n",
            "!chown -R daemon:daemon elasticsearch-7.10.2\n",
            "!sudo -u daemon ES_JAVA_OPTS=\"-Xms512m -Xmx512m\" ./elasticsearch-7.10.2/bin/elasticsearch -d > es.log 2>&1 &\n"
        ])
    if "cassandra" in services:
        desc.append("- **Cassandra**")
        code.extend([
            "# Start Cassandra\n",
            "!wget -q https://archive.apache.org/dist/cassandra/4.1.3/apache-cassandra-4.1.3-bin.tar.gz\n",
            "!tar xf apache-cassandra-4.1.3-bin.tar.gz\n",
            "!apache-cassandra-4.1.3/bin/cassandra -R > cassandra.log 2>&1 &\n"
        ])
    if "minio" in services:
        desc.append("- **MinIO**")
        code.extend([
            "# Start MinIO\n",
            "!wget -q https://dl.min.io/server/minio/release/linux-amd64/minio\n",
            "!chmod +x minio\n",
            "!mkdir -p /content/minio_data\n",
            "!MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server /content/minio_data --console-address \":9001\" &> minio.log &\n"
        ])
    
    code.append("\nimport time, socket, os\n")
    code.append("def wait_for_port(port, host='localhost', timeout=120):\n")
    code.append("    start_time = time.time()\n")
    code.append("    while True:\n")
    code.append("        try:\n")
    code.append("            with socket.create_connection((host, port), timeout=1):\n")
    code.append("                print(f\"Service at {host}:{port} is ready!\")\n")
    code.append("                return True\n")
    code.append("        except (OSError, ConnectionRefusedError):\n")
    code.append("            if time.time() - start_time > timeout:\n")
    code.append("                print(f\"Timeout waiting for {host}:{port} to start.\")\n")
    code.append("                # Dump logs for debugging\n")
    code.append("                if os.path.exists('minio.log'):\n")
    code.append("                    print('--- MINIO LOG ---')\n")
    code.append("                    print(open('minio.log').read())\n")
    code.append("                if os.path.exists('es.log'):\n")
    code.append("                    print('--- ES LOG ---')\n")
    code.append("                    print(open('es.log').read())\n")
    code.append("                if os.path.exists('cassandra.log'):\n")
    code.append("                    print('--- CASSANDRA LOG ---')\n")
    code.append("                    print(open('cassandra.log').read())\n")
    code.append("                raise Exception(f\"Service at {host}:{port} failed to start.\")\n")
    code.append("            time.sleep(2)\n")
    code.append("\n")
    code.append("# Wait for services\n")
    if "kafka" in services: code.append("wait_for_port(9092) # Kafka\n")
    if "cassandra" in services: 
        code.append("wait_for_port(9042) # Cassandra\n")
        code.append("time.sleep(10) # Extra buffer for Cassandra\n")
    if "minio" in services: 
        code.append("wait_for_port(9000) # MinIO\n")
        code.append("time.sleep(5) # Extra buffer for MinIO\n")
    if "es" in services: code.append("wait_for_port(9200) # Elasticsearch\n")
    if "redis" in services: code.append("wait_for_port(6379) # Redis\n")
    if "mongo" in services: code.append("wait_for_port(27017) # MongoDB\n")
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
            "source": ["## 3. Create Kafka Topic\n\nCreates a topic named `input-topic`."]
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

# --- Scenarios NB61-NB70 (Renumbered from 41-50) ---

# 61: AdTech Bidding
def get_nb61():
    setup = get_service_start_block(["kafka", "redis", "cassandra"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Bid Simulator)\n\nSimulates bid requests with `bid_id`, `user_id`, `site`."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import threading\n",
                "import time, json, random\n",
                "from kafka import KafkaProducer\n",
                "\n",
                "def send_data():\n",
                "    producer = None\n",
                "    # Retry connection\n",
                "    while not producer:\n",
                "        try:\n",
                "            producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "        except Exception as e:\n",
                "            print(f\"Waiting for Kafka... {e}\")\n",
                "            time.sleep(2)\n",
                "    \n",
                "    # Send Loop\n",
                "    while True:\n",
                "        try:\n",
                "            bid = {'bid_id': f'b{random.randint(1000,99999)}', 'user_id': f'u{random.randint(1,10)}', 'site': 'example.com', 'bid_floor': random.uniform(0.1, 1.0)}\n",
                "            producer.send('input-topic', json.dumps(bid).encode('utf-8'))\n",
                "            time.sleep(0.1)\n",
                "        except Exception as e:\n",
                "            print(f\"Producer Error: {e}\")\n",
                "            time.sleep(1)\n",
                "\n",
                "print(\"Starting Bid Simulator (Background Thread)...\")\n",
                "t = threading.Thread(target=send_data)\n",
                "t.daemon = True\n",
                "t.start()\n",
                "print(\"Producer running continuously...\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Bid Decision Engine (Spark)\n\n1. Reads Bids from Kafka.\n2. Looks up User Profile in Redis (`standard` vs `premium`).\n3. Makes a BID/PASS decision.\n4. Logs the decision to Cassandra (`adtech.bids`)."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import json\n",
        "from pyspark.sql.functions import col, from_json\n",
        "from pyspark.sql.types import StructType, StructField, StringType, FloatType\n",
        "import redis\n",
        "from cassandra.cluster import Cluster\n",
        "\n",
        "# Setup Redis & Cassandra\n",
        "r = redis.Redis()\n",
        "for i in range(1, 11): r.set(f\"u{i}\", \"premium\" if i % 2 == 0 else \"standard\")\n",
        "\n",
        "cluster = Cluster(['127.0.0.1'])\n",
        "session = cluster.connect()\n",
        "session.execute(\"CREATE KEYSPACE IF NOT EXISTS adtech WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}\")\n",
        "session.execute(\"CREATE TABLE IF NOT EXISTS adtech.bids (bid_id text PRIMARY KEY, user_type text, action text)\")\n",
        "session.shutdown()\n",
        "\n",
        "spark = SparkSession.builder.appName(\"AdTech\").getOrCreate()\n",
        "spark.sparkContext.setLogLevel(\"WARN\")\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    if not rows: return\n",
        "    r_local = redis.Redis()\n",
        "    cluster_local = Cluster(['127.0.0.1'])\n",
        "    session_local = cluster_local.connect('adtech')\n",
        "    for row in rows:\n",
        "        data = json.loads(row.value)\n",
        "        bid_id = data['bid_id']\n",
        "        user_id = data['user_id']\n",
        "        # Lookup\n",
        "        u_type = r_local.get(user_id)\n",
        "        u_type = u_type.decode('utf-8') if u_type else 'unknown'\n",
        "        # Decision\n",
        "        action = \"BID\" if u_type == \"premium\" else \"PASS\"\n",
        "        # Log\n",
        "        session_local.execute(f\"INSERT INTO bids (bid_id, user_type, action) VALUES ('{bid_id}', '{u_type}', '{action}')\")\n",
        "    session_local.shutdown()\n",
        "    print(f\"Batch {epoch_id} processed {len(rows)} logic. Decisions logged to Cassandra.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nQuery Cassandra for bid logs."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from cassandra.cluster import Cluster\n",
                "import time\n",
                "\n",
                "print(\"Verifying Cassandra data...\")\n",
                "cluster = Cluster(['127.0.0.1'])\n",
                "session = cluster.connect('adtech')\n",
                "\n",
                "# Retry logic for eventual consistency\n",
                "for _ in range(5):\n",
                "    rows = list(session.execute(\"SELECT * FROM bids LIMIT 10\"))\n",
                "    if rows: break\n",
                "    time.sleep(2)\n",
                "\n",
                "print(f\"--- Found {len(rows)} Ad Bids ---\")\n",
                "for row in rows: print(row)\n",
                "\n",
                "session.shutdown()\n",
                "assert len(rows) > 0, \"Verification Failed: No data found in Cassandra!\""
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 62: Smart City Traffic
def get_nb62():
    # Remove default "minio" from standard block to provide custom setup
    setup = get_service_start_block(["kafka", "redis"])
    
    # Custom MinIO Start on Port 9010
    minio_custom = {
        "cell_type": "code",
        "metadata": {},
        "source": [
            "# Start MinIO on Custom Port 9010 to avoid conflicts\n",
            "!wget -q https://dl.min.io/server/minio/release/linux-amd64/minio\n",
            "!chmod +x minio\n",
            "!mkdir -p /content/minio_data_nb62\n",
            "!MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server /content/minio_data_nb62 --address \":9010\" --console-address \":9011\" &> minio_9010.log &\n",
            "\n",
            "# Wait for MinIO 9010\n",
            "import time, socket, os\n",
            "print('Waiting for MinIO on 9010...')\n",
            "start = time.time()\n",
            "while True:\n",
            "    try:\n",
            "        with socket.create_connection(('localhost', 9010), timeout=1): break\n",
            "    except (OSError, ConnectionRefusedError):\n",
            "        if time.time() - start > 120:\n",
            "             if os.path.exists('minio_9010.log'): print(open('minio_9010.log').read())\n",
            "             raise Exception('MinIO 9010 Failed')\n",
            "        time.sleep(1)\n",
            "print('MinIO 9010 Ready!')"
        ]
    }
    setup.append(minio_custom)

    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Traffic Sensors)\n\nSimulates speed data from sensors."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Traffic Simulator...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 sensor readings...\")\n",
                "for _ in range(500):\n",
                "    data = {'sensor_id': f's{random.randint(1,5)}', 'speed': random.randint(0, 120)}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Traffic Control Pipeline\n\n1. Calcs Avg Speed per batch.\n2. Updates Traffic Light status in Redis (Green/Red).\n3. Archives raw batch to MinIO (Port 9010)."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis, json\n",
        "from minio import Minio\n",
        "\n",
        "# Init MinIO (Port 9010)\n",
        "m_client = Minio(\"127.0.0.1:9010\", access_key=\"minioadmin\", secret_key=\"minioadmin\", secure=False)\n",
        "if not m_client.bucket_exists(\"traffic-archive\"): m_client.make_bucket(\"traffic-archive\")\n",
        "\n",
        "spark = SparkSession.builder.appName(\"SmartCity\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    data = [json.loads(r.value) for r in df.collect()]\n",
        "    if not data: return\n",
        "    \n",
        "    # Agg Logic\n",
        "    avg_speed = sum(d['speed'] for d in data) / len(data)\n",
        "    \n",
        "    # Redis Update\n",
        "    r = redis.Redis()\n",
        "    status = \"GREEN\" if avg_speed > 40 else \"RED\"\n",
        "    r.set(\"traffic:status\", status)\n",
        "    \n",
        "    # MinIO Archive\n",
        "    import io\n",
        "    content = json.dumps(data).encode('utf-8')\n",
        "    m_client.put_object(\"traffic-archive\", f\"batch_{epoch_id}.json\", io.BytesIO(content), len(content))\n",
        "    print(f\"Batch {epoch_id}: Avg Speed {avg_speed:.1f} -> {status}. Archived to MinIO.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Redis Status and MinIO Archives."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "from minio import Minio\n",
                "r = redis.Redis()\n",
                "print(f\"Traffic Status: {r.get('traffic:status')}\")\n",
                "\n",
                "m = Minio(\"127.0.0.1:9010\", access_key=\"minioadmin\", secret_key=\"minioadmin\", secure=False)\n",
                "print(f\"Archives: {len(list(m.list_objects('traffic-archive')))} files.\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 63: SIEM
def get_nb63():
    # Helper for Custom MinIO Setup (Port 9010)
    setup = get_service_start_block(["kafka", "es"])
    minio_custom = {
        "cell_type": "code",
        "metadata": {},
        "source": [
            "# Start MinIO on Custom Port 9010 (Shared with NB62 convention)\n",
            "!wget -q https://dl.min.io/server/minio/release/linux-amd64/minio\n",
            "!chmod +x minio\n",
            "!mkdir -p /content/minio_data_nb63\n",
            "!MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server /content/minio_data_nb63 --address \":9010\" --console-address \":9011\" &> minio_9010.log &\n",
            "\n",
            "# Wait for MinIO 9010\n",
            "import time, socket, os\n",
            "print('Waiting for MinIO on 9010...')\n",
            "start = time.time()\n",
            "while True:\n",
            "    try:\n",
            "        with socket.create_connection(('localhost', 9010), timeout=1): break\n",
            "    except (OSError, ConnectionRefusedError):\n",
            "        if time.time() - start > 120:\n",
            "             if os.path.exists('minio_9010.log'): print(open('minio_9010.log').read())\n",
            "             raise Exception('MinIO 9010 Failed')\n",
            "        time.sleep(1)\n",
            "print('MinIO 9010 Ready!')"
        ]
    }
    setup.append(minio_custom)
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Security Logs)\n\nSimulates log events with levels INFO/WARN/ERROR."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Security Log Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 logs...\")\n",
                "levels = ['INFO', 'WARN', 'ERROR']\n",
                "for _ in range(500):\n",
                "    log = {'timestamp': time.time(), 'level': random.choice(levels), 'msg': 'Activity detected'}\n",
                "    producer.send('input-topic', json.dumps(log).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Security Event Pipeline\n\n1. Filters ERROR logs for immediate indexing in Elasticsearch (`alerts`).\n2. (Placeholder) Could archive other logs to MinIO."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "from elasticsearch import Elasticsearch\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"SIEM\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    es = Elasticsearch(['http://localhost:9200'])\n",
        "    for row in rows:\n",
        "        log = json.loads(row.value)\n",
        "        if log['level'] == 'ERROR':\n",
        "            # Hot Storage\n",
        "            es.index(index='alerts', body=log)\n",
        "    print(f\"Batch {epoch_id} processed: Analyzed security events.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCount Alerts in Elasticsearch."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from elasticsearch import Elasticsearch\n",
                "es = Elasticsearch(['http://localhost:9200'])\n",
                "time.sleep(2)\n",
                "try:\n",
                "    res = es.count(index=\"alerts\")\n",
                "    print(f\"Alerts Found: {res['count']}\")\n",
                "except: print(\"No alerts found yet.\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 64: E-commerce Fulfillment
def get_nb64():
    setup = get_service_start_block(["kafka", "redis", "mongo"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Orders)\n\nSimulates order placement."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Order Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 orders...\")\n",
                "for i in range(100):\n",
                "    order = {'order_id': i, 'item': f'item_{random.randint(1,5)}', 'qty': 1}\n",
                "    producer.send('input-topic', json.dumps(order).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Fulfillment Engine\n\n1. Checks Redis Inventory.\n2. Decrements if available.\n3. Confirms order to MongoDB or Fails it."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis, json\n",
        "from pymongo import MongoClient\n",
        "\n",
        "# Setup Inventory\n",
        "r = redis.Redis()\n",
        "for i in range(1,6): r.set(f\"inv:item_{i}\", 10)\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Ecommerce\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r_local = redis.Redis()\n",
        "    mongo = MongoClient()\n",
        "    db = mongo.shop\n",
        "    for row in rows:\n",
        "        order = json.loads(row.value)\n",
        "        item = order['item']\n",
        "        # Decr Inventory\n",
        "        new_qty = r_local.decr(f\"inv:{item}\")\n",
        "        if new_qty >= 0:\n",
        "            order['status'] = 'CONFIRMED'\n",
        "            db.orders.insert_one(order)\n",
        "        else:\n",
        "            order['status'] = 'FAILED'\n",
        "            db.failed_orders.insert_one(order)\n",
        "    print(f\"Batch {epoch_id} processed: Inventory updated & Orders recorded.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Confirmed vs Failed orders in Mongo."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from pymongo import MongoClient\n",
                "m = MongoClient()\n",
                "print(f\"Confirmed Orders: {m.shop.orders.count_documents({})}\")\n",
                "print(f\"Failed Orders: {m.shop.failed_orders.count_documents({})}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 65: Social Sentiment
def get_nb65():
    setup = get_service_start_block(["kafka", "redis", "es"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Tweets)\n\nSimulates social media posts with hashtags."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Tweet Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 tweets...\")\n",
                "tags = ['#happy', '#sad', '#neutral']\n",
                "for _ in range(500):\n",
                "    data = {'text': f'I am feeling {random.choice(tags)}', 'tag': random.choice(tags)}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Sentiment Dashboard\n\n1. Aggregates counts by tag locally.\n2. Updates Redis counters (Hash `sentiment_counts`)."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis, json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Social\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    data = [json.loads(r.value) for r in df.collect()]\n",
        "    if not data: return\n",
        "    r = redis.Redis()\n",
        "    for d in data:\n",
        "        r.hincrby(\"sentiment_counts\", d['tag'], 1)\n",
        "    print(f\"Batch {epoch_id} updated counts in Redis.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nRead sentiment counts from Redis."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "r = redis.Redis()\n",
                "print(r.hgetall(\"sentiment_counts\"))"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 66: Banking Fraud
def get_nb66():
    setup = get_service_start_block(["kafka", "cassandra", "redis"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Transactions)\n\nSimulates cc transactions."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Transaction Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 transactions...\")\n",
                "for _ in range(500):\n",
                "    # High amount = potential fraud\n",
                "    data = {'card': f'c{random.randint(1,10)}', 'amount': random.randint(10, 5000)}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Fraud Rules Engine\n\n1. Reads tx.\n2. If amount > 2000, publishes to Redis channel `fraud_alerts` and pushes to List `fraud_history` for audit."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"BankFraud\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r = redis.Redis()\n",
        "    for row in rows:\n",
        "        tx = json.loads(row.value)\n",
        "        if tx['amount'] > 2000:\n",
        "            # Publish Alert\n",
        "            r.publish('fraud_alerts', f\"Suspicious: {tx}\")\n",
        "            r.rpush('fraud_history', json.dumps(tx))\n",
        "            print(f\"FRAUD DETECTED: {tx}\")\n",
        "    print(f\"Batch {epoch_id} processed.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Fraud History List in Redis."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "r = redis.Redis()\n",
                "alerts = r.lrange('fraud_history', 0, -1)\n",
                "print(f\"--- {len(alerts)} Fraud Alerts Recorded ---\")\n",
                "for a in alerts[:5]: print(a)"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 67: Healthcare Vitals
def get_nb67():
    setup = get_service_start_block(["kafka", "mongo", "redis"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (IoT Vitals)\n\nSimulates BPM from patients."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Vitals Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 vitals...\")\n",
                "for _ in range(500):\n",
                "    data = {'patient': 'p1', 'bpm': random.randint(60, 150)}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Health Monitor\n\n1. Archives all data to MongoDB (`health.vitals`).\n2. Pushes alert to Redis List if BPM > 120."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis\n",
        "from pymongo import MongoClient\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Health\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r = redis.Redis()\n",
        "    mongo = MongoClient()\n",
        "    for row in rows:\n",
        "        vital = json.loads(row.value)\n",
        "        # Archive\n",
        "        mongo.health.vitals.insert_one(vital)\n",
        "        # Alert\n",
        "        if vital['bpm'] > 120:\n",
        "             r.lpush('alerts', f\"HIGH BPM: {vital['bpm']}\")\n",
        "    print(f\"Batch {epoch_id} processed: Archived to Mongo & Alerts checked.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Mongo and Redis Alerts."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from pymongo import MongoClient\n",
                "import redis\n",
                "m = MongoClient()\n",
                "print(f\"Vitals Recorded: {m.health.vitals.count_documents({})}\")\n",
                "r = redis.Redis()\n",
                "print(f\"Alerts Triggered: {r.llen('alerts')}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 68: Supply Chain
def get_nb68():
    setup = get_service_start_block(["kafka", "redis", "minio"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (RFID)\n\nSimulates package movement."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting RFID Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 tags...\")\n",
                "for i in range(100):\n",
                "    data = {'rfid': f'tag_{i}', 'loc': 'warehouse'}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Traceability Spark Job\n\nUpdates current location of each item in Redis."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Supply\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r = redis.Redis()\n",
        "    for row in rows:\n",
        "        item = json.loads(row.value)\n",
        "        r.set(f\"loc:{item['rfid']}\", item['loc'])\n",
        "    print(f\"Batch {epoch_id} updated locations.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck locations in Redis."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "r = redis.Redis()\n",
                "keys = r.keys(\"loc:*\")\n",
                "print(f\"Tracked Items: {len(keys)}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 69: Gaming Telemetry
def get_nb69():
    setup = get_service_start_block(["kafka", "redis", "cassandra"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer (Game Events)\n\nSimulates headshots."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Game Event Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 500 events...\")\n",
                "for _ in range(500):\n",
                "    data = {'player': f'p{random.randint(1,100)}', 'action': 'headshot'}\n",
                "    producer.send('input-topic', json.dumps(data).encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Anti-Cheat Engine\n\n1. Increments headshot count in Redis.\n2. If count > Threshold, adds player to `ban_list` set."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import redis\n",
        "import json\n",
        "\n",
        "spark = SparkSession.builder.appName(\"Gaming\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    r = redis.Redis()\n",
        "    for row in rows:\n",
        "        evt = json.loads(row.value)\n",
        "        count = r.incr(f\"hs:{evt['player']}\")\n",
        "        if count > 10: # Threshold\n",
        "             r.sadd(\"ban_list\", evt['player'])\n",
        "             print(f\"BANNED {evt['player']}\")\n",
        "    print(f\"Batch {epoch_id} processed.\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Ban List in Redis."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "import redis\n",
                "r = redis.Redis()\n",
                "banned = r.smembers(\"ban_list\")\n",
                "print(f\"Banned Players: {banned}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# 70: Multi-Cloud Replication
def get_nb70():
    setup = get_service_start_block(["kafka", "minio", "mongo", "es"])
    logic = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 4. Producer\n\nSends raw data to be replicated."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from kafka import KafkaProducer\n",
                "import json, time, random\n",
                "print(\"Starting Replication Producer...\")\n",
                "producer = KafkaProducer(bootstrap_servers='localhost:9092')\n",
                "print(\"Sending 100 items...\")\n",
                "for i in range(100):\n",
                "    producer.send('input-topic', f'data_{i}'.encode('utf-8'))\n",
                "producer.flush()\n",
                "print(\"Producer finished.\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 5. Replication Job\n\nReplicates each message to Mongo and Elasticsearch simultaneously."]
        }
    ]
    spark_code = [
        "from pyspark.sql import SparkSession\n",
        "import json\n",
        "from pymongo import MongoClient\n",
        "from elasticsearch import Elasticsearch\n",
        "\n",
        "spark = SparkSession.builder.appName(\"MultiCloud\").getOrCreate()\n",
        "\n",
        "def process_batch(df, epoch_id):\n",
        "    rows = df.collect()\n",
        "    if not rows: return\n",
        "    mongo = MongoClient()\n",
        "    es = Elasticsearch(['http://localhost:9200'])\n",
        "    for row in rows:\n",
        "        val = row.value.decode('utf-8')\n",
        "        # Replicate\n",
        "        mongo.cloud.replica.insert_one({'val': val})\n",
        "        es.index(index='replica', body={'val': val})\n",
        "    print(f\"Batch {epoch_id} replicated to Mongo & ES\")\n",
        "\n",
        "print(\"Starting Spark Streaming Job...\")\n",
        "df = spark.readStream.format(\"kafka\").option(\"kafka.bootstrap.servers\", \"localhost:9092\").option(\"subscribe\", \"input-topic\").option(\"startingOffsets\", \"earliest\").load()\n",
        "query = df.selectExpr(\"CAST(value AS STRING)\").writeStream.foreachBatch(process_batch).start()\n",
        "query.awaitTermination(30)\n",
        "print(\"Spark Job Finished.\")"
    ]
    logic.extend(get_spark_submit_block(spark_code))
    logic.extend([
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## 6. Verification\n\nCheck Replication counts."]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "from pymongo import MongoClient\n",
                "from elasticsearch import Elasticsearch\n",
                "\n",
                "m = MongoClient()\n",
                "print(f\"Mongo Count: {m.cloud.replica.count_documents({})}\")\n",
                "\n",
                "es = Elasticsearch(['http://localhost:9200'])\n",
                "time.sleep(2)\n",
                "print(f\"ES Count: {es.count(index='replica')['count']}\")"
            ]
        }
    ])
    return get_setup_env_block() + setup + get_topic_create_block() + logic

# --- Main ---
if __name__ == "__main__":
    create_notebook("61_adtech_bidding.ipynb", "NB61: Real-time AdTech Bidding", "Kafka -> Spark -> Redis/Cassandra", get_nb61())
    create_notebook("62_smart_city_traffic.ipynb", "NB62: Smart City Traffic", "Kafka -> Spark -> Redis/MinIO", get_nb62())
    create_notebook("63_siem_security.ipynb", "NB63: SIEM Log Analysis", "Kafka -> Spark -> ES/MinIO", get_nb63())
    create_notebook("64_ecommerce_fulfillment.ipynb", "NB64: E-commerce Fulfillment", "Kafka -> Spark -> Redis/Mongo", get_nb64())
    create_notebook("65_social_sentiment.ipynb", "NB65: Social Sentiment", "Kafka -> Spark -> Redis/ES", get_nb65())
    create_notebook("66_banking_fraud.ipynb", "NB66: Banking Fraud Detection", "Kafka -> Spark -> Cassandra/Redis", get_nb66())
    create_notebook("67_healthcare_vitals.ipynb", "NB67: Healthcare Vitals", "Kafka -> Spark -> Mongo/Redis", get_nb67())
    create_notebook("68_supply_chain.ipynb", "NB68: Supply Chain Traceability", "Kafka -> Spark -> Redis/MinIO", get_nb68())
    create_notebook("69_gaming_telemetry.ipynb", "NB69: Gaming Telemetry", "Kafka -> Spark -> Redis/Cassandra", get_nb69())
    create_notebook("70_multi_cloud_replication.ipynb", "NB70: Multi-Cloud Replication", "Kafka -> Spark -> MinIO/Mongo/ES", get_nb70())
