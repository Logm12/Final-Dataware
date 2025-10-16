# DataCo — SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS

Link to original dataset (Kaggle): https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis?resource=download

This repository contains a recommended Git-style project README and structure for implementing two complementary data services built from the DataCo dataset:

- Database Service A — Order Management System (OMS): manages customer & order lifecycle up to the point an order is ready for shipping (pre-logistics).
- Database Service B — Shipping & Logistics Management System (SLMS): manages shipping execution, tracking, routing and delivery performance (post-order).

Below you will find:
- Suggested repo layout
- Clear table schemas (DDL) for OMS and SLMS
- Suggested indexes and foreign keys
- Example JOIN queries and metrics
- ETL / data pipeline guidance
- Recommended "Essential" and "Advanced" algorithms and business solutions
- Deployment, privacy, and contribution notes

---

## Repository structure (suggested)

- data/                — raw and processed sample CSVs (NOT checked into VCS for large files)
- notebooks/           — exploration, modeling, examples (Jupyter/Colab)
- sql/                 — DDL, sample queries, views
- etl/                 — ETL scripts (Python / Airflow dags)
- services/
  - oms/               — OMS microservice code, migrations, tests
  - slms/              — SLMS microservice code, migrations, tests
- docs/                — diagrams, runbooks, API specs (OpenAPI)
- experiments/         — model training artifacts, evaluation results
- README.md            — this file
- LICENSE

---

## Database Schemas

Below are recommended SQL table definitions for the two services. Use types appropriate for your RDBMS (Postgres shown, adjust for MySQL, etc.).

sql/ddl_oms.sql (example)
```sql
-- OMS: Order Management System
CREATE TABLE customers (
  customer_id            BIGINT PRIMARY KEY,
  fname                  VARCHAR(128),
  lname                  VARCHAR(128),
  segment                VARCHAR(64),
  email                  VARCHAR(256),
  street                 VARCHAR(256),
  city                   VARCHAR(128),
  state                  VARCHAR(128),
  country                VARCHAR(128),
  zipcode                VARCHAR(32),
  created_at             TIMESTAMP
);

CREATE TABLE categories (
  category_id            INT PRIMARY KEY,
  category_name          VARCHAR(128),
  department_id          INT
);

CREATE TABLE departments (
  department_id          INT PRIMARY KEY,
  department_name        VARCHAR(128)
);

CREATE TABLE products (
  product_card_id        BIGINT PRIMARY KEY,
  product_name           VARCHAR(256),
  product_description    TEXT,
  product_category_id    INT REFERENCES categories(category_id),
  product_price          NUMERIC(12,2),
  product_status         VARCHAR(32),
  product_image          TEXT
);

CREATE TABLE orders (
  order_id               BIGINT PRIMARY KEY,
  customer_id            BIGINT REFERENCES customers(customer_id),
  order_date             DATE,
  order_status           VARCHAR(64),
  order_city             VARCHAR(128),
  order_state            VARCHAR(128),
  order_country          VARCHAR(128),
  order_zipcode          VARCHAR(32)
);

CREATE TABLE order_items (
  order_item_id          BIGINT PRIMARY KEY,
  order_id               BIGINT REFERENCES orders(order_id),
  product_card_id        BIGINT REFERENCES products(product_card_id),
  unit_price             NUMERIC(12,2),
  discount               NUMERIC(12,2),
  discount_rate          NUMERIC(5,2),
  quantity               INT,
  total                  NUMERIC(12,2),
  profit                 NUMERIC(12,2),
  profit_ratio           NUMERIC(6,4)
);

-- Useful materialized views for sales metrics can be created in sql/views/
```

sql/ddl_slms.sql (example)
```sql
-- SLMS: Shipping & Logistics Management System
CREATE TABLE shipments (
  shipment_id            BIGINT PRIMARY KEY,
  order_id               BIGINT, -- FK to OMS.orders.order_id (join across services)
  shipping_mode          VARCHAR(64),
  shipping_date          DATE,
  days_for_shipping_real INT,    -- actual shipping days
  days_for_shipment_sched INT,   -- scheduled days (from OMS or SLA)
  late_delivery_risk     BOOLEAN,
  delivery_status        VARCHAR(64),
  carrier                VARCHAR(128),
  created_at             TIMESTAMP
);

CREATE TABLE tracking_points (
  id                     BIGINT PRIMARY KEY,
  shipment_id            BIGINT REFERENCES shipments(shipment_id),
  recorded_at            TIMESTAMP,
  latitude               NUMERIC(10,6),
  longitude              NUMERIC(10,6),
  location_city          VARCHAR(128),
  location_state         VARCHAR(128),
  location_country       VARCHAR(128)
);

CREATE TABLE order_shipment_link (
  order_id               BIGINT PRIMARY KEY,
  shipment_id            BIGINT REFERENCES shipments(shipment_id),
  customer_id            BIGINT -- optional replicated FK for analytical joins
);
```

Indexing & constraints (recommendations)
- Index orders(order_date), orders(customer_id)
- Index order_items(order_id), order_items(product_card_id)
- Index shipments(order_id), shipments(shipping_date), shipments(delivery_status)
- Spatial/geospatial indexing for tracking_points (PostGIS) if needed
- Foreign key constraints between services can be enforced at application level if services are decoupled; consider read-replicas or a central analytics DB for consolidated joins.

---

## Sample queries & analytics

1) Join OMS + SLMS to compute late deliveries by region
```sql
SELECT o.order_id,
       o.order_date,
       o.order_city,
       s.shipment_id,
       s.shipping_date,
       s.delivery_status,
       s.days_for_shipping_real,
       s.late_delivery_risk
FROM orders o
JOIN order_shipment_link osl ON o.order_id = osl.order_id
JOIN shipments s ON osl.shipment_id = s.shipment_id
WHERE s.late_delivery_risk = true;
```

2) Customer lifetime value (high-level)
```sql
SELECT c.customer_id,
       COUNT(DISTINCT o.order_id) AS orders,
       SUM(oi.total) AS total_spend,
       AVG(oi.total) AS avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id;
```

3) Delivery performance by carrier
```sql
SELECT s.carrier,
       COUNT(*) AS shipments,
       AVG(s.days_for_shipping_real) as avg_days,
       SUM(CASE WHEN s.late_delivery_risk THEN 1 ELSE 0 END)::float / COUNT(*) as late_rate
FROM shipments s
GROUP BY s.carrier
ORDER BY late_rate DESC;
```

---

## ETL / Data pipeline guidance

- Source: Kaggle CSVs or operational exports from OMS/SLMS.
- Ingest step: store raw files in data/raw/ (S3, GCS, or local).
- Transform step:
  - Normalize columns, parse dates, deduplicate orders and tracking points.
  - Enrich: geocode addresses if needed, map product categories to standard taxonomy.
  - Compute derived features: order lead time, scheduled_vs_actual_days, distance traveled, avg speed, time-in-transit.
- Load step:
  - Load cleaned data into analytical warehouse (Postgres, Redshift, BigQuery, Snowflake).
  - Maintain incremental load using upserts and CDC (change data capture).
- Orchestration: Airflow/Prefect/Kubeflow depending on scale.
- Monitoring: data quality checks, SLA alerts, anomaly detection.

---

## Business solutions: Essential and Advanced Algorithms

The following algorithms are grouped into Essential (fast to implement, high ROI) and Advanced (higher complexity, higher potential impact).

Essential (quick wins)
- Sales & Demand
  - Time series forecasting (Prophet / ARIMA) for weekly/monthly sales forecasts.
  - Exponential smoothing for SKU-level demand smoothing.
- Customer & Order
  - RFM segmentation + K-means for customer cohorts.
  - Rule-based CLTV heuristics and simple Gamma-Gamma + BG/NBD for probabilistic CLTV.
- Delivery & Logistics
  - Classification (Logistic Regression, XGBoost) for late_delivery_risk.
  - ETA estimation: Gradient boosted trees (XGBoost / LightGBM) on feature set (distance, carrier, historical delays, shipment size).
- Anomaly detection
  - Isolation Forest on per-shipment features for fraud/unusual delays.

Advanced (higher effort / impact)
- Deep learning time series (LSTM / TCN / Transformer-based) for SKU-level demand forecasting with cross-series learning.
- Reinforcement learning for dynamic routing and inventory replenishment policies.
- Multi-armed bandits for price/discount optimization and promotional A/B testing.
- Graph-based route optimization with OR-Tools + heuristics for multi-stop delivery scheduling.
- Advanced geospatial models: spatio-temporal models for route risk prediction, using Graph Neural Networks for complex route/state interactions.
- Causal inference / uplift modeling to measure effect of promotions on order volume and profit.
- End-to-end pipeline for supply chain digital twin: simulation + optimization.

Feature engineering suggestions (for ML)
- Temporal: order weekday, hour, holiday flag, seasonality lags
- Customer: prior order frequency, average order value, churn risk
- Product: SKU popularity, shelf-life proxy, return rate
- Route/shipping: distance (Haversine), avg speed, historical delay rate by route/carrier
- Environmental: weather (attach NOAA), traffic indices, local events

Evaluation & KPI suggestions
- Forecasts: MAPE, RMSE, weighted MAPE by revenue
- Classification: ROC-AUC, Precision@k, confusion matrix for late risk
- Routing/Logistics: % on-time, avg delivery time, cost per delivery
- Business: revenue lift, profit margin, inventory days-of-stock

---

## Integration patterns and architecture notes

- Microservices: OMS and SLMS should be microservices with well-defined APIs:
  - OMS: create/read/update orders, items, customers; publish "order_ready_for_shipping" events.
  - SLMS: subscribe to order events, create shipments, update tracking points, expose delivery status endpoints.
- Event-driven pipeline: use Kafka / RabbitMQ / cloud-managed pub/sub for order -> shipment handoff and near-real-time analytics.
- Analytical data store: replicate or ETL operational stores into a data warehouse optimized for analytic joins and modeling.
- Data contracts: define versioned schemas for events (e.g., JSON Schema/Avro) to avoid coupling issues.

---

## Data governance & privacy

- Treat personally identifiable information (PII) with care (customer names, emails, addresses).
- Use encryption at rest and in transit; redact or tokenise PII in analytics datasets where possible.
- Follow GDPR/CCPA where applicable: allow customer deletion / right to be forgotten requests.
- Maintain audit logs for data access and transformations.

---

## Deployment & scaling suggestions

- Start with single-node Postgres + small Celery/Airflow for ETL; scale to cloud-managed warehouse (BigQuery/Redshift/Snowflake) as volume grows.
- Use container orchestration (Kubernetes) for service deployments; autoscale consumers for tracking ingestion bursts.
- Attach a streaming pipeline for real-time metrics (Kafka + ksqlDB / Flink for continuous aggregations).

---

## Example notebooks & experiments

Place example notebooks in notebooks/ including:
- EDA notebook: basic cleaning, joins between OMS and SLMS, visualizations of late_delivery_risk by city/carrier.
- Model notebook: baseline XGBoost for late_delivery_risk + feature importance.
- Forecast notebook: baseline Prophet per-category forecast and aggregated evaluation.

---

## How to run locally (quickstart)

1. Clone this repo.
2. Put raw CSVs into data/raw/ (from Kaggle link above).
3. Create a Python virtualenv and install requirements (pandas, sqlalchemy, psycopg2, scikit-learn, xgboost, prophet).
4. Run ETL sample: python etl/ingest_sample.py — this loads small CSVs into local Postgres.
5. Run notebook examples in notebooks/.

(Provide concrete scripts in etl/ to make steps reproducible.)

---

## Contribution & roadmap

- Good first issues:
  - Add SQL views for common KPIs (sales_by_region, latency_by_carrier).
  - Implement baseline models for late_delivery_risk and ETA.
- Mid-term:
  - Integrate geospatial (PostGIS) for route analytics.
  - Implement event-driven microservices with example Docker Compose.
- Long-term:
  - RL-based routing, digital twin simulation, causal uplift experiments.

---

## References & further reading

- Kaggle dataset: link above
- Prophet docs (forecasting): https://facebook.github.io/prophet/
- XGBoost / LightGBM docs
- Google OR-Tools (routing)
- BG/NBD + Gamma-Gamma for customer lifetime value (LTV) modeling

---
