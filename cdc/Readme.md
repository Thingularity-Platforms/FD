# MySQL CDC Pipeline — Local to Microsoft Fabric

A Change Data Capture (CDC) pipeline that streams MySQL changes through Kafka into Delta Lake tables, with a Bronze → Silver medallion architecture. Built locally with Docker and designed to migrate to Microsoft Fabric / Azure Event Hubs.

---

## Architecture

### Local Setup
```
MySQL (Docker)
    ↓  binlog
Debezium / Kafka Connect (Docker)
    ↓  CDC events
Kafka (Docker)
    ↓  Kafka protocol
Spark Structured Streaming — main.py        [localhost:4040]
    ↓  upsert / delete
Bronze Delta Table  (./delta/orders)
    ↓  Delta stream
Spark Structured Streaming — silver_main.py [localhost:4041]
    ↓
Silver Delta Table          (./silver/orders)
Silver Rejected Delta Table (./silver/orders_rejected)
```

---

## Project Structure

```
cdc/
├── config.py               # All constants — paths, Kafka, Spark packages
├── spark_session.py        # SparkSession factory (local + Fabric aware)
├── kafka_reader.py         # Kafka source reader + Debezium parser
├── delta_writer.py         # Bronze Delta upsert/delete logic
├── main.py                 # Bronze job entry point
├── maintenance.py          # Z-order + compaction + vacuum
├── verify.py               # Quick data verification script
├── docker-compose.yml      # MySQL + Kafka + Debezium containers
└── silver/
    ├── cleansing.py        # Standardize, deduplicate, validate, split
    ├── silver_writer.py    # Silver Delta upsert + rejected append
    └── silver_main.py      # Silver job entry point
```

---

## Prerequisites

| Component | Where | Version |
|---|---|---|
| Java JDK | Windows (native) | 17 |
| Python | Windows (native) | 3.10 |
| Apache Spark | Windows (native) | 3.5.8 |
| winutils | Windows (native) | 3.3+ |
| PySpark | Windows (pip) | 3.5.8 |
| delta-spark | Windows (pip) | 3.2.0 |
| Docker Desktop | Windows | Latest |
| MySQL | Docker | 8.0 |
| Kafka + Zookeeper | Docker | Confluent 7.5.0 |
| Debezium | Docker | 2.4 |

---

## Windows Installation Guide

### Step 1 — Java JDK 17

1. Download JDK 17 from [Oracle](https://www.oracle.com/java/technologies/downloads/#java17) or use [Microsoft OpenJDK](https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-17)
2. Run the installer — default path is `C:\Program Files\Java\jdk-17`
3. Set environment variables:

```powershell
# Open System Environment Variables → New
JAVA_HOME = C:\Program Files\Java\jdk-17

# Edit Path → Add
%JAVA_HOME%\bin
```

Verify:
```powershell
java -version
# Expected: java version "17.x.x"
```

---

### Step 2 — Python 3.10

1. Download from [python.org](https://www.python.org/downloads/release/python-3100/)
2. Run installer — check **"Add Python to PATH"** during setup
3. Verify:

```powershell
python --version
# Expected: Python 3.10.x
```

---

### Step 3 — Apache Spark 3.5.8

1. Download Spark 3.5.8 with Hadoop 3 from [spark.apache.org](https://spark.apache.org/downloads.html)
   - Choose: `Spark 3.5.8` → `Pre-built for Apache Hadoop 3.3`
   - File: `spark-3.5.8-bin-hadoop3.tgz`

2. Extract to `C:\spark` — your path should look like:
   ```
   C:\spark\bin\
   C:\spark\jars\
   C:\spark\python\
   ```

3. Set environment variables:

```powershell
# New variable
SPARK_HOME = C:\spark

# Edit Path → Add
%SPARK_HOME%\bin
```

Verify:
```powershell
spark-submit --version
# Expected: version 3.5.8
```

---

### Step 4 — winutils (Required for Spark on Windows)

Spark needs `winutils.exe` to interact with the filesystem on Windows — without it Spark will throw Hadoop errors.

1. Download `winutils.exe` for Hadoop 3.3 from [GitHub — cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin)
2. Create folder `C:\hadoop\bin\`
3. Place `winutils.exe` inside `C:\hadoop\bin\`
4. Set environment variable:

```powershell
# New variable
HADOOP_HOME = C:\hadoop

# Edit Path → Add
%HADOOP_HOME%\bin
```

Verify:
```powershell
winutils.exe
# Should print usage info, not an error
```

---

### Step 5 — Python Packages

```powershell
pip install pyspark==3.5.8 delta-spark==3.2.0
```

Verify:
```powershell
python -c "import pyspark; print(pyspark.__version__)"
# Expected: 3.5.8
```

---

### Step 6 — Docker Desktop

1. Download from [docker.com](https://www.docker.com/products/docker-desktop/)
2. Run installer
3. During setup — enable **WSL2 backend** (recommended) or Hyper-V
4. Restart your machine after install
5. Open Docker Desktop and wait for it to show **"Engine running"**

Verify:
```powershell
docker --version
docker-compose --version
```

---

---

### Step 8 — Verify Full Environment

Run this checklist before proceeding:

```powershell
java -version          # 17.x.x
python --version       # 3.10.x
spark-submit --version # 3.5.8
winutils.exe           # prints usage
docker ps              # no errors
python -c "import pyspark; print(pyspark.__version__)"   # 3.5.8
python -c "import delta; print(delta.__version__)"       # 3.2.0
```

---

## Docker Compose

Save this as `docker-compose.yml` in your project root (`cdc/`):

```yaml
version: '3.8'

services:

  mysql:
    image: mysql:8.0
    container_name: mysql-cdc
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: sourcedb
      MYSQL_USER: cdcuser
      MYSQL_PASSWORD: cdcpass
    ports:
      - "3306:3306"
    command: >
      --server-id=1
      --log-bin=mysql-bin
      --binlog-format=ROW
      --binlog-row-image=FULL
      --expire-logs-days=10
    volumes:
      - mysql-data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-proot123"]
      interval: 10s
      timeout: 5s
      retries: 5

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 15s
      timeout: 10s
      retries: 5

  kafka-connect:
    image: debezium/connect:2.4
    container_name: kafka-connect
    depends_on:
      kafka:
        condition: service_healthy
      mysql:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:29092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: debezium_configs
      OFFSET_STORAGE_TOPIC: debezium_offsets
      STATUS_STORAGE_TOPIC: debezium_status
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8083/connectors"]
      interval: 15s
      timeout: 10s
      retries: 5

volumes:
  mysql-data:
```

---

## Quick Start

Once all prerequisites are installed, the full pipeline starts in 3 terminals:

```powershell
# Terminal 1 — infrastructure
docker-compose up -d

# Wait ~30 seconds, then register Debezium
Invoke-RestMethod -Method POST -Uri http://localhost:8083/connectors -ContentType "application/json" -Body '{"name":"mysql-cdc-connector","config":{"connector.class":"io.debezium.connector.mysql.MySqlConnector","tasks.max":"1","database.hostname":"mysql","database.port":"3306","database.user":"cdcuser","database.password":"cdcpass","database.server.id":"184054","topic.prefix":"cdc","database.include.list":"sourcedb","decimal.handling.mode":"double","schema.history.internal.kafka.bootstrap.servers":"kafka:29092","schema.history.internal.kafka.topic":"schema-changes.sourcedb"}}'

# Terminal 2 — Bronze streaming job
python main.py

# Terminal 3 — Silver streaming job
python silver/silver_main.py
```

**Key design decisions:**

| Decision | Reason |
|---|---|
| `healthcheck` on every service | Ensures `kafka-connect` only starts after Kafka and MySQL are truly ready — prevents the `TimeoutException` on startup |
| Two Kafka listeners (`INTERNAL`/`EXTERNAL`) | `kafka:29092` for Debezium inside Docker, `localhost:9092` for Spark on Windows host |
| `condition: service_healthy` | Waits for actual service readiness, not just container start |
| `REPLICATION_FACTOR: 1` | Prevents warnings on a single-broker setup |

---

## Pipeline Setup Guide

### 1. Start Docker Services

```powershell
docker-compose up -d
```

This starts MySQL, Zookeeper, Kafka, and Kafka Connect (Debezium). Wait ~30 seconds for all services to be healthy.

Verify all containers are running:
```powershell
docker ps
```

Expected containers: `mysql-cdc`, `zookeeper`, `kafka`, `kafka-connect`

### 2. Create MySQL Table

```powershell
docker exec -it mysql-cdc mysql -uroot -proot123
```

```sql
USE sourcedb;

CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_name VARCHAR(100),
  amount DECIMAL(10,2),
  status VARCHAR(20),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcuser'@'%';
FLUSH PRIVILEGES;
```

### 3. Register Debezium Connector

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8083/connectors -ContentType "application/json" -Body '{
  "name": "mysql-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "cdcuser",
    "database.password": "cdcpass",
    "database.server.id": "184054",
    "topic.prefix": "cdc",
    "database.include.list": "sourcedb",
    "decimal.handling.mode": "double",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schema-changes.sourcedb"
  }
}'
```

Verify connector status:
```powershell
Invoke-RestMethod -Method GET -Uri http://localhost:8083/connectors/mysql-cdc-connector/status
```

Both `connector.state` and `tasks[0].state` should be `RUNNING`.

### 4. Run Bronze Streaming Job

```powershell
cd C:\Users\<YOU>\Desktop\pyspark\cdc
python main.py
```

Spark UI available at: `http://localhost:4040`

### 5. Run Silver Streaming Job

Open a new terminal:
```powershell
cd C:\Users\<YOU>\Desktop\pyspark\cdc
python silver/silver_main.py
```

Spark UI available at: `http://localhost:4041`

### 6. Verify Data

Open a third terminal:
```powershell
cd C:\Users\<YOU>\Desktop\pyspark\cdc
python verify.py
```

---

## Data Flow Detail

### Debezium CDC Envelope

Every MySQL change is wrapped by Debezium as:
```json
{
  "payload": {
    "before": { "id": 1, "status": "NEW" },
    "after":  { "id": 1, "status": "COMPLETED" },
    "op": "u",
    "ts_ms": 1234567890
  }
}
```

| `op` value | Meaning |
|---|---|
| `c` | INSERT |
| `u` | UPDATE |
| `d` | DELETE |
| `r` | Snapshot read (initial load) |

### Bronze Layer
- Raw CDC data exactly as received from MySQL
- All inserts, updates and deletes applied via Delta MERGE
- No cleansing — source of truth

### Silver Layer
- Reads Bronze Delta table as a stream
- Applies standardization, deduplication and validation
- Clean records → `silver/orders`
- Bad records with rejection reason → `silver/orders_rejected`

#### Cleansing Rules Applied

| Rule | Action |
|---|---|
| `id` is NULL | Reject — Missing id |
| `customer_name` is NULL | Reject — Missing customer_name |
| `amount` is NULL | Reject — Missing amount |
| `amount` <= 0 | Reject — Invalid amount |
| `status` is NULL | Reject — Missing status |
| `status` not in allowed values | Reject — Invalid status value |
| Extra whitespace in strings | Trim |
| Duplicate records for same `id` | Keep latest by `updated_at` |

#### Valid Status Values
`NEW` · `PENDING` · `COMPLETED` · `CANCELLED`

---

## Kafka Network Configuration

Two listeners are configured to handle both internal (Docker) and external (Windows host) access:

| Listener | Address | Used By |
|---|---|---|
| `INTERNAL` | `kafka:29092` | Debezium (inside Docker) |
| `EXTERNAL` | `localhost:9092` | Spark (on Windows host) |

---

## Maintenance

Run Z-order optimization, compaction and vacuum manually after bulk loads or on a schedule:

```powershell
python maintenance.py
```

| Operation | Purpose |
|---|---|
| `OPTIMIZE ZORDER` | Co-locate rows by merge key `id` — speeds up MERGE scans |
| Compaction | Merges small files produced by streaming into larger ones |
| `VACUUM` | Removes old Delta versions older than 168 hours (7 days) |

**Recommended frequency:**

| Volume | Frequency |
|---|---|
| POC / low | Manually after bulk inserts |
| Production small | Every few hours |
| Production high | Every 30 minutes |

---

## Test Data

Insert records to cover all cleansing scenarios:

```sql
-- Valid records
INSERT INTO orders (customer_name, amount, status) VALUES ('Alice Johnson', 150.00, 'NEW');
INSERT INTO orders (customer_name, amount, status) VALUES ('Bob Smith', 299.99, 'PENDING');
INSERT INTO orders (customer_name, amount, status) VALUES ('Carol White', 49.50, 'COMPLETED');
INSERT INTO orders (customer_name, amount, status) VALUES ('David Brown', 999.00, 'CANCELLED');

-- Rejected: NULL customer_name
INSERT INTO orders (customer_name, amount, status) VALUES (NULL, 100.00, 'NEW');

-- Rejected: NULL amount
INSERT INTO orders (customer_name, amount, status) VALUES ('Eve Davis', NULL, 'NEW');

-- Rejected: zero amount
INSERT INTO orders (customer_name, amount, status) VALUES ('Frank Miller', 0, 'NEW');

-- Rejected: negative amount
INSERT INTO orders (customer_name, amount, status) VALUES ('Grace Lee', -50.00, 'PENDING');

-- Rejected: invalid status
INSERT INTO orders (customer_name, amount, status) VALUES ('Henry Wilson', 200.00, 'SHIPPED');

-- Deduplication test
INSERT INTO orders (customer_name, amount, status) VALUES ('Jack Martin', 500.00, 'NEW');
UPDATE orders SET status = 'PENDING'   WHERE customer_name = 'Jack Martin';
UPDATE orders SET status = 'COMPLETED' WHERE customer_name = 'Jack Martin';

-- Trim test
INSERT INTO orders (customer_name, amount, status) VALUES ('   Karen Clark   ', 120.00, 'NEW');
```

---

---

## Version Compatibility

| Component | Version |
|---|---|
| Apache Spark | 3.5.8 |
| PySpark | 3.5.8 |
| delta-spark | 3.2.0 |
| delta-core | 2.4.0 |
| spark-sql-kafka | 3.5.0 |
| Scala | 2.12 |
| Java | 17 |
| Python | 3.10 |
| Debezium | 2.4 |
| Confluent Kafka | 7.5.0 |
| MySQL | 8.0 |
