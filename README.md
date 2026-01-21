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

### Como Executar (Google Colab)
1. Faça upload do notebook desejado (`notebooks/*.ipynb`) para o Google Colab.
2. Execute todas as células (`Runtime > Run all`).
3. O notebook instalará automaticamente o Java, Spark, Kafka, Redis e Mongo necessários.

### Como Executar (Localmente)
Para os scripts Python (`scripts/*.py`):
1. Instale as dependências: `pip install -r requirements.txt`
2. Garanta que Java 11+ esteja instalado.
3. Execute: `python3 scripts/01_aviation_telemetry.py`

- The final cell triggers the streaming job using `!spark-submit`.

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

### Phase 6: Advanced Integrations
- **21. E-commerce**: Recommendations (Spark + Redis + Mongo)
- **22. Dynamic Pricing**: Real-time Pricing (Spark + Redis + Mongo)
- **23. Fraud Detection**: Location Anomaly (Spark + Redis + Mongo)
- **24. Smart Home**: IoT Monitoring (Spark + Redis + Mongo)
- **25. Logistics**: Fleet Tracking (Spark + Redis + Mongo)
- **26. Gaming**: Player State (Spark + Redis + Mongo)
- **27. Finance**: Trading Bot (Spark + Redis + Mongo)
- **28. Social Media**: Trend Counter (Spark + Redis + Mongo)
- **29. Healthcare**: Monitor V2 (Spark + Redis + Mongo)
- **30. Cyber Security**: Threat Intel (Spark + Redis + Mongo)

### Phase 7: Apache Flink (PyFlink)
- **31. WordCount**: Contador de palavras (Batch/Stream)
- **32. Fraud Detection**: Filtro de transações suspeitas
- **33. IoT Windows**: Janelas de tempo (Tumbling)
- **34. Enrichment**: Enriquecimento de Streams
- **35. SQL Dashboard**: Processamento declarativo com Flink SQL

### Phase 8: Big Data Ops (Spark + Elasticsearch + Presto)
- **36. Log Analytics**: Pipeline de logs (Spark -> ES -> Presto)
- **37. Product Catalog**: Busca Facetada (Spark -> ES)
- **38. SIEM Threat Hunting**: Segurança em Tempo Real (Spark Streaming -> ES)
- **39. Customer 360**: Agregação de Perfil (Spark -> ES)
- **40. Hybrid Analytics**: Consultas Federadas (Presto JOIN ES + TPCH)

### Material Extra
- **Kafka Avançado (Deep Dive)**: Tutorial detalhado sobre arquitetura Kafka (Log, Partições, Consumer Groups, Semântica de Entrega) - `notebooks/Kafka_Avancado_Colab.ipynb`

### Phase 9: Distributed Pipelines (Colab Optimized)
These 20 notebooks (NB51-NB70) are specifically designed to run **full distributed pipelines inside Google Colab**. Each notebook sets up its own environment (Kafka, Spark, Redis, Mongo, Cassandra, Elasticsearch, MinIO) and executes a complete streaming logic (Producer -> Spark -> Sink).
**New Features (v2):**
- **Automated Verification**: End-of-notebook cells programmatically verify output (JSON/DB/Logs).
- **Progress Visibility**: Clear "Starting...", "Processing...", "Finished" logs for all Producers and Consumers.
- **Robust Startup**: Intelligent port checking ensures services (Cassandra, Kafka) are fully ready before execution.
- **Dependency Fixes**: Pinned versions (e.g., `numpy<2.0.0`) to ensure compatibility with all libraries.

**Batch 1: Core Technologies (NB51-NB60)**
- **NB51**: Kafka & Spark Basics (Streaming Hello World)
- **NB52**: Spark + Cassandra (Time Series Sensor Data)
- **NB53**: Spark + Elasticsearch (Log Analysis Pipeline)
- **NB54**: Spark + MinIO (Data Lake / S3 Sink)
- **NB55**: Spark + Redis (Low-latency Enrichment)
- **NB56**: Spark + MongoDB (Document Sink)
- **NB57**: Hot (ES) vs Cold (MinIO) Architecture
- **NB58**: Fraud Detection with Redis State
- **NB59**: IoT Aggregation to Cassandra
- **NB60**: End-to-End Pipeline (Valid -> Mongo, Invalid -> DLQ)

**Batch 2: Full Solution Pipelines (NB61-NB70)**
- **NB61**: **AdTech Bidding**: Bid Requests -> User Profile Lookup (Redis) -> Bid Decision -> Log (Cassandra).
- **NB62**: **Smart City**: Traffic Sensors -> Avg Speed Aggregation -> Traffic Light Control (Redis) + Archival (MinIO).
- **NB63**: **SIEM Security**: Logs -> Severity Filter -> Alert Indexing (ES) + Cold Storage (MinIO).
- **NB64**: **E-commerce**: Orders -> Inventory Check (Redis) -> Fulfillment (Mongo) or Fail.
- **NB65**: **Social Sentiment**: Stream -> Sentiment Calc -> Dashboard Stats (Redis) + Search Index (ES).
- **NB66**: **Banking Fraud**: Transactions -> Historical Feature Lookup (Cassandra) -> ML Rules -> Alert PubSub (Redis).
- **NB67**: **Healthcare**: Vitals -> Windowed Avg -> Patient History (Mongo) + Real-time Alert (Redis).
- **NB68**: **Supply Chain**: RFID Events -> Path Validation (Redis) -> Audit Log (MinIO).
- **NB69**: **Gaming Ops**: Telemetry -> Cheat Detection logic -> Ban List (Redis) + Replay Store (Cassandra).
- **NB70**: **Multi-Cloud**: On-Prem Stream -> Replication to Object Store (MinIO) + NoSQL (Mongo) + Search (ES).

## Upload to Google Drive

To easily run these notebooks in Google Colab, you can upload them to your Google Drive automatically.

1.  **Dependencies**: Install the upload script requirements:
    ```bash
    pip install -r scripts/requirements_drive.txt
    ```

2.  **Credentials**: 
    - Download your OAuth 2.0 Client ID JSON from Google Cloud Console.
    - Save it as `scripts/client_secrets.json`.

3.  **Run Upload**:
    ```bash
    python3 scripts/upload_to_drive.py
    ```
    - The first run will open a browser for authentication.
    - It creates a folder `Colab_Notebooks` (or uses one defined in `scripts/.env`).

