# DataStream 

A high-performance, concurrent financial market data pipeline built from scratch in Go. This project was developed as a hands-on journey to learn Go's powerful concurrency primitives, time-series database management, and low-latency network communication frameworks.

DataStream ingests live streaming data from both real-time WebSocket push feeds (Binance) and REST API poll targets (Yahoo Finance), normalizes the payloads into standardized Protobuf formats, manages system backpressure safely via concurrent worker pools, and distributes real-time market ticks over gRPC Server-Side Streams.

---

## Core Engineering Features

* **Advanced Go Concurrency Engine:** Leverages raw Goroutines and buffered Go channels to handle millions of transactions with backpressure mitigation. 
* **Multi-Modal Data Ingestion:** Seamlessly unifies multi-threaded WebSocket push architectures with interval-based HTTP REST API poll mechanisms.
* **Stateful Deduplication Logic:** Implements custom state gates to evaluate transactional updates. When traditional stock exchanges freeze operations overnight, the pipeline halts redundant time-series row allocation.
* **Thread-Safe In-Memory Pub/Sub Cache:** Utilizes low-level memory synchronization gates (`sync.RWMutex`) to establish a real-time message broker directly in RAM, reducing read query latency to sub-millisecond intervals.
* **TimescaleDB Partitioned Engine:** Integrates specialized PostgreSQL container schemas optimized for real-time, high-density time-series analytical processing.
* **gRPC Server-Side Streaming Engine:** Utilizes HTTP/2 communication protocols via Protocol Buffers to establish continuous real-time streaming connections to downstream services.

---

## Quick Start Guide

### 1. Prerequisites
Ensure you have **Go (1.26+)** and **Docker Desktop** installed on your machine.

### 2. Launch Infrastructure
Boot up the time-series database environment container:
```bash
docker-compose up -d
```

### 3. Verify Database Connectivity
Execute the independent database verification smoke script to ensure tables and hyper-table conversions are configured:
```bash
go run cmd/testdb/main.go
```

### 4. Boot the DataStream Server
Start the central market ingestor and gRPC server framework:
```bash
go run cmd/server/main.go
```

### 5. Attach the Streaming Client
Open an independent terminal window and execute the client wrapper to see live trade data stream over gRPC:
```bash
go run cmd/client/main.go
```

---

## Future Scope

As DataStream progresses from a learning project into a production-grade project, several advanced features remain open for implementation:

* **Full App Containerization & Orchestration:** Extend the current Docker Compose setup by creating an optimized multi-stage `Dockerfile` for the Go application so the entire cluster deploys with a single script execution.
* **Cross-Language Client Bridges (Python SDK):** Compile the `.proto` service contract into Python gRPC stubs, allowing machine learning models, Jupyter Notebooks, or automated algorithmic systems to ingest live price updates into Pandas DataFrames.
* **Telemetry and Observability Dashboards:** Add internal Prometheus metric tracking hooks inside the ingestor worker routines and wire them to a Grafana interface to visualize real-time pipeline telemetry like CPU usage, ingestion latency, and memory performance.