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

### Local
| Tool | Version |
|---|---|
| Docker Desktop | Latest (WSL2 backend) |
| Python | 3.10 |
| Java | 17 |
| Apache Spark | 3.5.8 |
| PySpark | 3.5.8 |
| delta-spark | 3.2.0 |

```bash
pip install pyspark==3.5.8 delta-spark==3.2.0
```

### Environment Variables
Set in `config.py` — update the Python path to match your machine:
```python
os.environ['PYSPARK_PYTHON'] = r'C:\Users\<YOU>\AppData\Local\Programs\Python\Python310\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\<YOU>\AppData\Local\Programs\Python\Python310\python.exe'
```

---

## Local Setup Guide

### 1. Start Docker Services

```bash
docker-compose up -d
```

This starts MySQL, Zookeeper, Kafka, and Kafka Connect (Debezium).

Verify all containers are running:
```bash
docker ps
```

### 2. Create MySQL Table

```bash
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
python main.py
```

Spark UI available at: `http://localhost:4040`

### 5. Run Silver Streaming Job

Open a new terminal:
```powershell
python silver/silver_main.py
```

Spark UI available at: `http://localhost:4041`

### 6. Verify Data

```powershell
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

```

Only `config.py` needs to change — all other files are environment-agnostic.



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
