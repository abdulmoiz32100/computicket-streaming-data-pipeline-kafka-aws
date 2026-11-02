# Computicket Near-Real-Time Streaming Ingestion Pipeline

An event-driven data streaming pipeline designed to replace high-latency browser-based batch scrapers with sub-second WebSocket event ingestion, buffered via Apache Kafka on AWS EC2, and sunk directly to Amazon S3 for serverless SQL analytics via AWS Glue and Amazon Athena.

---

## 🏗️ Architecture Overview<img width="2752" height="1536" alt="Computickets Data Pipeline Using Kafka" src="https://github.com/user-attachments/assets/e07c99e5-4db8-4681-83ce-8cd16666b227" />



```text
[Computicket WebSocket Stream] 
       │ (JSON Payloads)
       ▼
[Python Kafka Producer] 
       │ (Async Event Stream)
       ▼
[Apache Kafka on AWS EC2] ── (Topic: Ticket-DataPipeline-Topic)
       │ (Distributed Broker)
       ▼
[Python Kafka Consumer]
       │ (Direct Partitioned Sink)
       ▼
[Amazon S3 Raw Lake] ── (date=YYYY-MM-DD/)
       │
       ▼
[AWS Glue Crawler & Data Catalog]
       │ (Inferred Schema)
       ▼
[Amazon Athena] ── (Interactive SQL Analytics)
