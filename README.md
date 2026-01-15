# 20 Distributed Processing Examples

This repository contains 20 examples of distributed data processing pipelines using **Apache Spark**, **Dask**, and **Kafka**.
The examples cover various industries and use cases, demonstrating streaming and batch processing.

## Structure
- `notebooks/`: Jupyter Notebooks (`.ipynb`) compatible with Google Colab.
- `scripts/`: Native Python scripts (`.py`) for local execution.
- `data/`: Directory where processed data is persisted (SQLite and Parquet).

## Prerequisites (Local Execution)

1. **Python 3.8+**
2. **Java 8+** (Required for Spark and Kafka)
3. **Kafka** (Running locally on port 9092 for Spark examples)
   - *Note: Dask examples (csv-based) do not require Kafka.*

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Starting Kafka (Linux/Mac)
If you need to run the Spark Streaming examples:
```bash
# Assuming you have Kafka installed or downloaded
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
```

## Running Examples

### Option 1: Google Colab (Notebooks)
Upload any `.ipynb` file from `notebooks/` to Colab. 

**Note for Spark Notebooks:**
The Spark notebooks have been updated to write consumer code to `kafka_consumer.py` and execute via `spark-submit` for stability.
- Ensure you run the `Installation` and `Start Kafka` cells first.
- The final cell triggers the streaming job using `!spark-submit`.

### Option 2: Local Python Scripts
Run the scripts directly. They will start a background producer (simulator) and a consumer/processor.

**Example (Dask - No Kafka needed):**
```bash
python3 scripts/02_automotive_iot.py
```
*Output will appear in console. Data saved to `data/automotive.db` and `data/automotive_iot.parquet`.*

**Example (Spark - Requires Kafka):**
```bash
python3 scripts/01_aviation_telemetry.py
```

## Output Data
The scripts persist data to:
- **SQLite Databases**: `data/*.db` (For structured data / dashboards)
- **Parquet Files**: `data/*.parquet` (For bulk historical data)

## List of Examples

### Phase 1: Core Sectors
1.  **Aviation**: Telemetry (Spark)
2.  **Automotive**: IoT Sensors (Dask)
3.  **Healthcare**: Vitals Monitor (Spark)
4.  **Sales**: Fraud Detection (Spark ML)
5.  **Smart City**: Traffic Control (Dask)

### Phase 2: Industry
6.  **Finance**: Stock Analysis (Spark)
7.  **Social Media**: Sentiment Analysis (Spark)
8.  **Logistics**: Route Opt (Dask)
9.  **Energy**: Smart Grid (Spark)
10. **Manufacturing**: Predictive Maint (Dask)

### Phase 3: Services
11. **Cybersecurity**: IDS (Spark)
12. **Agriculture**: Irrigation (Dask)
13. **Gaming**: Leaderboard (Spark)
14. **Telecom**: Network Quality (Dask)
15. **Retail**: Inventory (Spark)

### Phase 4: Daily Life
16. **Weather**: Heatwave Alerts (Dask)
17. **Ride-Sharing**: Dynamic Pricing (Spark)
18. **Hospitality**: Revenue Dash (Dask)
19. **Insurance**: Risk Analysis (Spark)
20. **Education**: Proctoring (Dask)
