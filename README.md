# 🚀 Distributed Log Processing System with Control-Aware Anomaly Detection

---

## 🧠 Problem

Modern distributed log processing systems are designed to handle high-throughput data streams, but they often fail under real-world conditions such as high load and cascading failures. The primary issues observed in such systems include retry amplification, where failed requests are retried aggressively leading to exponential load increase, and queue saturation, where incoming requests exceed processing capacity, causing unbounded latency growth.

Additionally, most systems lack a feedback-driven control mechanism. They continue operating blindly under stress without regulating input or processing behavior, eventually leading to collapse instead of graceful degradation.

This project addresses these issues by designing a system that remains stable under stress, controls failure amplification, and intelligently detects instability before it leads to failure.

---

## 🎯 Objective

The goal of this system is not just to process logs, but to **maintain control under stress**. Specifically, the system is designed to:

* Sustain high throughput (up to 10K logs/sec) without collapsing
* Prevent retry storms by bounding and regulating retries
* Control queue growth using adaptive feedback mechanisms
* Maintain bounded latency even under overload conditions
* Detect early signs of instability instead of reacting after failure
* Clearly distinguish between high load (normal behavior) and system failure (loss of control)

---

## 🏗️ System Architecture

![Architecture](assets/architecture.png)

```text
Producer → Kafka → Consumers → Retry Queue → DLQ → Processing → Redis → Control Loop → Anomaly Detection
```

The system follows an event-driven distributed architecture where each component operates independently but is coordinated through messaging and shared state.

* Producers generate logs at configurable rates
* Kafka acts as a buffer and decoupling layer
* Consumers process logs in parallel
* Retry and DLQ pipelines handle failures separately
* Redis stores metrics and system state
* A control loop continuously monitors system behavior and regulates it

---

## ⚙️ Core Components

### 🔹 Kafka-Based Log Pipeline

Kafka serves as the backbone of the system, enabling asynchronous and scalable communication between components. Logs are partitioned and processed in parallel, allowing the system to handle high throughput efficiently. The use of separate topics for main logs, retries, and dead-letter queues ensures clean separation of concerns and prevents failure scenarios from affecting the entire pipeline.

---

### 🔹 Bounded Retry System

Retries are one of the most dangerous components in distributed systems if left uncontrolled. Instead of blindly retrying failed requests, this system implements a bounded retry mechanism:

* Each message is retried only a limited number of times (2–3)
* Exponential backoff with jitter is applied to prevent synchronized retry bursts
* Retry amplification is tracked as a first-class metric

This ensures that retries help recovery without becoming a source of system instability.

---

### 🔹 Circuit Breaker

The circuit breaker protects the system from sustained failure conditions. It transitions between three states:

* **CLOSED**: Normal operation, retries allowed
* **OPEN**: Retries are stopped to prevent resource waste
* **HALF-OPEN**: Limited retries allowed to test recovery

The circuit breaker is triggered based on system-level signals such as high failure rate, increased retry amplification, and elevated latency. This prevents the system from continuously retrying operations that are unlikely to succeed.

---

### 🔹 Adaptive Backpressure

Backpressure is used to regulate the rate at which the system processes logs. Instead of allowing uncontrolled ingestion, the system dynamically adjusts processing behavior based on current load and system health.

When latency or queue pressure increases, the system reduces processing intensity, allowing it to stabilize. This ensures that the system operates within its capacity limits and avoids collapse.

---

### 🔹 Control Loop (Core Innovation)

![Control Loop](assets/control-loop.png)

The control loop is the central intelligence of the system. It continuously monitors metrics such as latency, retry amplification, and queue pressure, and adjusts system behavior accordingly.

Unlike traditional systems that operate passively, this system actively regulates itself, ensuring stability even under adverse conditions.

---

## 📊 System Behavior (Validated)

![Load vs Latency](assets/load-vs-latency.png)

Through extensive testing, the system exhibits the following behavior:

| Load     | Behavior                                     |
| -------- | -------------------------------------------- |
| 1–2K/sec | Stable operation with low latency            |
| 3–4K/sec | Maximum sustainable throughput               |
| ~5K/sec  | Saturation begins, latency increases         |
| 10K/sec  | Controlled saturation, system remains stable |

The key observation is that the system does not collapse under high load. Instead, it transitions into a controlled saturation state where latency increases but remains bounded.

---

## 🔍 Key Insights

Several important insights emerged from building and testing this system:

* System failure is primarily caused by **queue saturation**, not retries
* Latency is driven by **backlog accumulation**, not just processing time
* High load alone is not a problem — lack of control is
* Feedback mechanisms are essential for maintaining stability
* Distributed systems require active regulation, not passive execution

---

## 🚨 Anomaly Detection (Control-Aware)

![Anomaly Detection](assets/anomaly.png)

### Core Principle

> The system detects **loss of control**, not high load

---

### Fast Signals (Immediate Stress Detection)

These signals indicate sudden stress in the system:

* High queue pressure
* Retry amplification spikes
* Latency spikes

They provide early warning but do not necessarily indicate failure.

---

### Slow Signals (True Instability)

These signals detect sustained divergence over time:

* Continuous latency growth across multiple windows
* Persistent queue pressure
* Increasing retry instability

These indicate that the system is losing control and approaching failure.

---

### Important Behavior

* High load does NOT trigger anomalies
* Only sustained instability triggers anomalies
* The system avoids false positives by distinguishing stress from failure

---

## ⚠️ Critical Edge Case

### Recovery Phase Distortion

During recovery, an interesting issue was observed:

* Ingestion latency includes delays from past backlog
* Pipeline latency reflects current processing conditions

This mismatch leads to incorrect signals such as artificially high queue pressure.

---

### Resolution

To address this:

* Metrics are validated before use
* Phase-aware filtering is applied
* Time-window consistency is enforced

This ensures accurate anomaly detection across different system phases.

---

## 📈 Results

The system demonstrates significant improvements over naive implementations:

* Retry amplification reduced from ~3x to ~1.1x
* Latency growth changed from exponential to bounded
* System remains stable under 10K logs/sec
* Retry storms are eliminated
* System collapse is prevented

---

## 🧠 System Behavior Model

The system operates in three distinct phases:

### 1. Overload Phase

* Latency increases
* Queue builds up
* Retry attempts increase

---

### 2. Controlled Saturation

* Latency stabilizes
* Queue growth is bounded
* Control loop actively regulates system

---

### 3. Recovery Phase

* Retry rate decreases
* Backlog is cleared
* Latency returns to normal

---

## ⚔️ Final Takeaway

> The system does not detect load — it detects loss of control.

---

## 🧠 What This Demonstrates

This project demonstrates a deep understanding of:

* Distributed systems design
* Control loop engineering
* Failure handling under load
* System stability and resilience
* Real-world backend system behavior

---

## 🚀 Future Work

Potential improvements include:

* Multi-service dependency modeling
* Kafka lag-based anomaly detection
* Predictive failure analysis using trends
* Long-term metric storage and analysis

