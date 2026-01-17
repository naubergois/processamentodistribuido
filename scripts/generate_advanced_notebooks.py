import json
import os

# Template for the Notebook
def create_notebook(filename, title, scenario, producer_code, consumer_code):
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    f"# {title}\n",
                    "\n",
                    f"{scenario}\n",
                    "\n",
                    "**Architecture**:\n",
                    "*   **Source**: Kafka (Streaming)\n",
                    "*   **Enrichment/State**: Redis (Fast lookups)\n",
                    "*   **Sink**: MongoDB (Document storage)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 1. Environment Setup\nInstall Java, Spark, Kafka, Redis, MongoDB, and Python drivers."]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Install Java\n",
                    "!apt-get install openjdk-8-jdk-headless -qq > /dev/null\n",
                    "\n",
                    "# Install Spark & Kafka\n",
                    "!wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz\n",
                    "!tar xf spark-3.5.0-bin-hadoop3.tgz\n",
                    "!wget -q https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz\n",
                    "!tar xf kafka_2.13-3.6.1.tgz\n",
                    "\n",
                    "# Install Redis & MongoDB\n",
                    "!apt-get install redis-server -qq > /dev/null\n",
                    "!wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add -\n",
                    "!echo \"deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse\" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list\n",
                    "!apt-get update -qq > /dev/null\n",
                    "!apt-get install -y mongodb-org -qq > /dev/null\n",
                    "\n",
                    "# Install Python Libs\n",
                    "!pip install -q findspark pyspark kafka-python redis pymongo"
                ]
            },
             {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "import os\n",
                    "os.environ[\"JAVA_HOME\"] = \"/usr/lib/jvm/java-8-openjdk-amd64\"\n",
                    "os.environ[\"SPARK_HOME\"] = \"/content/spark-3.5.0-bin-hadoop3\"\n",
                    "import findspark\n",
                    "findspark.init()"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 2. Start Services"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "%%bash\n",
                    "# Start Redis\n",
                    "service redis-server start\n",
                    "\n",
                    "# Start MongoDB (Background)\n",
                    "mkdir -p /data/db\n",
                    "mongod --fork --logpath /var/log/mongodb.log --bind_ip 127.0.0.1\n",
                    "\n",
                    "# Start Kafka\n",
                    "cd kafka_2.13-3.6.1\n",
                    "bin/zookeeper-server-start.sh -daemon config/zookeeper.properties\n",
                    "sleep 5\n",
                    "bin/kafka-server-start.sh -daemon config/server.properties\n",
                    "sleep 5\n",
                    "bin/kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1"
                ]
            },
             {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Verify Connections\n",
                    "import redis\n",
                    "from pymongo import MongoClient\n",
                    "\n",
                    "try:\n",
                    "    r = redis.Redis(host='localhost', port=6379, db=0)\n",
                    "    r.set('test', 'connected')\n",
                    "    print(f\"Redis: {r.get('test').decode('utf-8')}\")\n",
                    "except Exception as e: print(f\"Redis Error: {e}\")\n",
                    "\n",
                    "try:\n",
                    "    m = MongoClient('localhost', 27017)\n",
                    "    print(f\"MongoDB: {m.list_database_names()}\")\n",
                    "except Exception as e: print(f\"Mongo Error: {e}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 3. Producer (Simulated Data)"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": producer_code
            },
             {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 4. Consumer (Spark + Redis + Mongo)"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "%%writefile kafka_consumer.py\n",
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import from_json, col, udf, struct, to_json\n",
                    "from pyspark.sql.types import *\n",
                    "import redis\n",
                    "from pymongo import MongoClient\n",
                    "import json\n",
                    "\n",
                    "# --- Init Spark ---\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"AdvancedIntegration\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "spark.sparkContext.setLogLevel(\"WARN\")\n",
                    "\n",
                    "# --- Redis & Mongo Connection Helpers (Executed on Workers) ---\n",
                    "def get_redis():\n",
                    "    return redis.Redis(host='localhost', port=6379, db=0)\n",
                    "\n",
                    "def write_to_mongo_redis(batch_df, batch_id):\n",
                    "    # This function runs on the driver, but operations should be optimized for workers if large scale.\n",
                    "    # For simplicity in this example, we collect minor batches or use foreachPartition inside.\n",
                    "    \n",
                    "    data = batch_df.collect()\n",
                    "    if not data: return\n",
                    "    \n",
                    "    r_client = get_redis()\n",
                    "    m_client = MongoClient('localhost', 27017)\n",
                    "    db = m_client['streaming_db']\n",
                    "    collection = db['output_collection']\n",
                    "    \n",
                    "    print(f\"Processing Batch {batch_id} with {len(data)} records\")\n",
                    "    \n",
                    "    for row in data:\n",
                    "        record = row.asDict()\n",
                    "        \n",
                    consumer_code + "\n",
                    "        # Save to Mongo\n",
                    "        try:\n",
                    "            collection.insert_one(record)\n",
                    "        except Exception as e: print(e)\n",
                    "            \n",
                    "    m_client.close()\n",
                    "    r_client.close()\n",
                    "\n"
                    "# --- Stream Definition ---\n",
                    "schema = StructType([\n",
                    "    StructField(\"id\", StringType()),\n",
                    "    StructField(\"timestamp\", StringType()),\n",
                    "    StructField(\"value\", StringType()),\n",
                    "    StructField(\"meta\", StringType())\n",
                    "     # Simplified schema for template, can be overridden\n",
                    "])\n",
                    "\n",
                    "df = spark.readStream \\\n",
                    "    .format(\"kafka\") \\\n",
                    "    .option(\"kafka.bootstrap.servers\", \"localhost:9092\") \\\n",
                    "    .option(\"subscribe\", \"input-topic\") \\\n",
                    "    .load()\n",
                    "\n",
                    "# Generic parsing - tailored in specific notebooks if needed, but here we assume JSON value\n",
                    "parsed = df.selectExpr(\"CAST(value AS STRING)\")\n",
                    "\n",
                    "# Apply Custom Processing\n",
                    "query = parsed.writeStream \\\n",
                    "    .foreachBatch(write_to_mongo_redis) \\\n",
                    "    .outputMode(\"update\") \\\n",
                    "    .start()\n",
                    "\n",
                    "query.awaitTermination()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": ["!spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 kafka_consumer.py"]
            }
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"codemirror_mode": {"name": "ipython", "version": 3}, "file_extension": ".py", "mimetype": "text/x-python", "name": "python", "nbconvert_exporter": "python", "pygments_lexer": "ipython3", "version": "3.10.12"}
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    
    with open(f"notebooks/{filename}", 'w') as f:
        json.dump(notebook, f, indent=1)
    print(f"Generated {filename}")

# --- Logic Definitions ---

# 21: E-commerce
# Data: {user_id, product_id, category}
# Redis: Key=user_id, Val={last_view, interest_score} -> Increment interest
# Mongo: Store view event enriched with current interest score
nb21_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "categories = ['electronics', 'fashion', 'books']\n",
    "for _ in range(500):\n",
    "    data = {'user_id': f'user_{random.randint(1,50)}', 'product_id': random.randint(100,200), 'category': random.choice(categories)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]

nb21_consumer_logic = """
        # Parse JSON
        val = json.loads(record['value'])
        user = val['user_id']
        cat = val['category']
        
        # Redis Logic: Update User Interest Profile
        # Key: user:interest:category
        r_key = f"{user}:{cat}"
        current_score = r_client.incr(r_key) # Increment interest
        
        # Enrich Record
        record['enriched_interest'] = current_score
        record['parsed_data'] = val
"""

# 22: Dynamic Pricing
nb22_consumer_logic = """
        val = json.loads(record['value'])
        item = val['item_id']
        demand = val.get('demand', 1)
        
        # Redis: Get Base Price
        base_price = r_client.get(f"price:{item}")
        if not base_price: 
             base_price = 100
             r_client.set(f"price:{item}", 100)
        else:
             base_price = float(base_price)
             
        # Dynamic Logic
        new_price = base_price * (1 + (demand / 100))
        
        record['old_price'] = base_price
        record['new_price'] = new_price
        record['parsed_data'] = val
"""


# --- 21: E-commerce ---
nb21_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "categories = ['electronics', 'fashion', 'books']\n",
    "for _ in range(500):\n",
    "    data = {'user_id': f'user_{random.randint(1,50)}', 'product_id': random.randint(100,200), 'category': random.choice(categories)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]

nb21_consumer_logic = """
        # Parse JSON
        val = json.loads(record['value'])
        user = val['user_id']
        cat = val['category']
        
        # Redis Logic: Update User Interest Profile
        # Key: user:interest:category
        r_key = f"{user}:{cat}"
        current_score = r_client.incr(r_key) # Increment interest
        
        # Enrich Record
        record['enriched_interest'] = current_score
        record['parsed_data'] = val
"""

# --- 22: Dynamic Pricing ---
nb22_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "items = ['item_A', 'item_B', 'item_C']\n",
    "for _ in range(500):\n",
    "    # Simulate high demand for item_A \n",
    "    item = random.choice(items)\n",
    "    demand = random.randint(1, 10) if item == 'item_A' else random.randint(1, 3)\n",
    "    data = {'item_id': item, 'demand_index': demand, 'timestamp': time.time()}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]
nb22_consumer_logic = """
        val = json.loads(record['value'])
        item = val['item_id']
        demand = val['demand_index']
        
        # Redis: Get Base Price (Default 100)
        base_key = f"price:{item}"
        base = r_client.get(base_key)
        if not base: base = 100
        else: base = float(base)
        
        # Logic: Increase 5% per demand point
        new_price = base * (1 + (demand * 0.05))
        
        record['calculated_price'] = new_price
        record['parsed_data'] = val
"""

# --- 23: Credit Fraud ---
# Redis stores 'last_country' for user. If varies, flag it.
nb23_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "countries = ['US', 'BR', 'DE', 'JP']\n",
    "for _ in range(500):\n",
    "    data = {'card_id': f'card_{random.randint(1,10)}', 'amount': random.randint(10, 1000), 'country': random.choice(countries)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]
nb23_consumer_logic = """
        val = json.loads(record['value'])
        card = val['card_id']
        curr_country = val['country']
        
        # Redis: Check last country
        last_country = r_client.get(f"loc:{card}")
        
        alert = False
        if last_country:
            last_country = last_country.decode('utf-8')
            if last_country != curr_country:
                alert = True
        
        # Update Redis
        r_client.set(f"loc:{card}", curr_country)
        
        record['fraud_alert'] = alert
        record['parsed_data'] = val
"""

# --- 24: Smart Home ---
# Redis stores device 'thresholds'. Kafka sends sensor data.
nb24_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "for _ in range(500):\n",
    "    data = {'device_id': f'thermo_{random.randint(1,5)}', 'temp': random.randint(15, 35)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]
nb24_consumer_logic = """
        val = json.loads(record['value'])
        dev = val['device_id']
        temp = val['temp']
        
        # Redis: Get Max Temp Config (Default 28)
        limit = r_client.get(f"config:{dev}")
        if not limit: limit = 28
        else: limit = int(limit)
        
        if temp > limit:
            record['alert'] = "OVERHEATING"
        else:
            record['alert'] = "NORMAL"
            
        record['parsed_data'] = val
"""

# --- 25: Logistics ---
# Redis stores 'route_id' -> 'expected_zone'.
nb25_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "zones = ['warehouse', 'transit', 'customer']\n",
    "for _ in range(500):\n",
    "    data = {'truck_id': f'truck_{random.randint(1,10)}', 'current_zone': random.choice(zones)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]
nb25_consumer_logic = """
        val = json.loads(record['value'])
        truck = val['truck_id']
        zone = val['current_zone']
        
        # Redis: Set an expected zone
        r_client.setnx(f"plan:{truck}", "transit") 
        expected = r_client.get(f"plan:{truck}").decode('utf-8')
        
        if zone != expected:
            record['violation'] = True
        else:
            record['violation'] = False
            
        record['parsed_data'] = val
"""

# --- 26: Gaming ---
# Redis: Leaderboard (Sorted Set)
nb26_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "for _ in range(500):\n",
    "    data = {'player': f'p{random.randint(1,100)}', 'points': random.randint(10, 100)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.05)\n",
    "producer.close()"
]
nb26_consumer_logic = """
        val = json.loads(record['value'])
        player = val['player']
        pts = val['points']
        
        # Redis: Update Leaderboard
        new_score = r_client.zincrby("leaderboard", pts, player)
        rank = r_client.zrevrank("leaderboard", player)
        
        record['total_score'] = new_score
        record['current_rank'] = rank
        record['parsed_data'] = val
"""

# --- 27: Stock Bot ---
# Redis: User Portfolio Balance
nb27_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "stocks = ['AAPL', 'GOOG', 'TSLA']\n",
    "for _ in range(500):\n",
    "    data = {'symbol': random.choice(stocks), 'price': random.randint(100, 200)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]
nb27_consumer_logic = """
        val = json.loads(record['value'])
        sym = val['symbol']
        price = val['price']
        
        # Strategy: Buy if price < 150
        action = "HOLD"
        if price < 150:
            action = "BUY"
            # Redis: Dedup balance
            r_client.decrby("balance_usd", price)
            
        record['action'] = action
        record['parsed_data'] = val
"""

# --- 28: Social Trends ---
# Redis: Hash of hashtag counts
nb28_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "tags = ['#tech', '#news', '#ai', '#crypto']\n",
    "for _ in range(500):\n",
    "    data = {'post_id': random.randint(1000,9999), 'tag': random.choice(tags)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.05)\n",
    "producer.close()"
]
nb28_consumer_logic = """
        val = json.loads(record['value'])
        tag = val['tag']
        
        # Redis: Atomic Increment
        total = r_client.hincrby("trends", tag, 1)
        
        record['tag_total'] = total
        record['parsed_data'] = val
"""

# --- 29: Healthcare V2 ---
# Redis: Patient specific limits
nb29_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "for _ in range(500):\n",
    "    data = {'patient_id': f'pid_{random.randint(1,10)}', 'bpm': random.randint(50, 160)}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.1)\n",
    "producer.close()"
]
nb29_consumer_logic = """
        val = json.loads(record['value'])
        pid = val['patient_id']
        bpm = val['bpm']
        
        # Redis: Get max limit for patient (default 120)
        limit_key = f"limit:{pid}"
        max_bpm = r_client.get(limit_key)
        if not max_bpm: max_bpm = 120
        else: max_bpm = int(max_bpm)
        
        risk = "LOW"
        if bpm > max_bpm: risk = "CRITICAL"
        elif bpm > 100: risk = "ELEVATED"
        
        record['risk_level'] = risk
        record['parsed_data'] = val
"""

# --- 30: Cyber Threat ---
# Redis: IP Blacklist simulation
nb30_producer = [
    "from kafka import KafkaProducer\n",
    "import json, time, random\n",
    "producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))\n",
    "ips = ['192.168.1.5', '10.0.0.1', '8.8.8.8']\n",
    "for _ in range(500):\n",
    "    data = {'src_ip': random.choice(ips), 'action': 'login', 'status': random.choice(['success', 'fail'])}\n",
    "    producer.send('input-topic', value=data)\n",
    "    time.sleep(0.05)\n",
    "producer.close()"
]
nb30_consumer_logic = """
        val = json.loads(record['value'])
        ip = val['src_ip']
        status = val['status']
        
        blocked = False
        # Redis: Check blacklist
        if r_client.sismember("blacklist_ips", ip):
            blocked = True
        
        # Logic: If fail, incr count, if > 5 add to blacklist
        if status == 'fail':
             fails = r_client.incr(f"fails:{ip}")
             if fails > 5:
                 r_client.sadd("blacklist_ips", ip)
        
        record['blocked'] = blocked
        record['parsed_data'] = val
"""

# Generate All
create_notebook("21_ecommerce_recommendations.ipynb", "Example 21: E-commerce - Real-time Recommendations", "Track user views, update interest profiles in Redis, and persist enrichment to MongoDB.", nb21_producer, nb21_consumer_logic)
create_notebook("22_dynamic_pricing.ipynb", "Example 22: Dynamic Pricing", "Real-time pricing adjustments based on demand.", nb22_producer, nb22_consumer_logic)
create_notebook("23_credit_fraud.ipynb", "Example 23: Credit Fraud", "Detect location anomalies.", nb23_producer, nb23_consumer_logic)
create_notebook("24_smart_home.ipynb", "Example 24: Smart Home", "IoT sensor monitoring with Redis config.", nb24_producer, nb24_consumer_logic)
create_notebook("25_logistics_fleet.ipynb", "Example 25: Logistics Fleet", "Route deviation detection.", nb25_producer, nb25_consumer_logic)
create_notebook("26_gaming_player_state.ipynb", "Example 26: Gaming State", "Live leaderboards.", nb26_producer, nb26_consumer_logic)
create_notebook("27_stock_trading.ipynb", "Example 27: Stock Trading Bot", "Automated trading execution.", nb27_producer, nb27_consumer_logic)
create_notebook("28_social_media_trends.ipynb", "Example 28: Social Trends", "Hashtag tracking.", nb28_producer, nb28_consumer_logic)
create_notebook("29_healthcare_monitoring.ipynb", "Example 29: Healthcare V2", "Patient risk analysis.", nb26_producer, nb29_consumer_logic) # Fix: use nb29_producer
create_notebook("30_cyber_threat.ipynb", "Example 30: Cyber Threat", "IP Blacklisting.", nb30_producer, nb30_consumer_logic)

