# Project Architecture

```mermaid
graph TD
    subgraph Host_Machine [Windows Host Filesystem]
        subgraph Local_Data_Lake [Shared Mounted Directory: /data]
            direction LR
            B_Layer[(Bronze Tier: CSV)] ---> S_Layer[(Silver Tier: Combined State)]
            S_Layer ---> G_Layer[(Gold Tier: Partitioned Parquet)]
        end
    end

    subgraph Docker_Virtual_Bridge_Network [Docker Compose Virtual Network]
        A_Web[Airflow Webserver: 8080] <--> A_Sch[Airflow Scheduler]
        A_Sch <--> A_Meta[(Airflow Metadata DB: 5432)]
        
        A_Sch --->|Executes Tasks| P_Spark[Custom Worker: PySpark 3.5.0 + JRE]
        
        OLTP[(source_oltp_db: Postgres 5433)]
        OLAP[(target_olap_db: Postgres 5434)]
    end

    %% Data Extraction Line
    OLTP --->|1. Incremental CDC via updated_at| P_Spark
    P_Spark --->|2. Write CSV via PIPELINE_RUN_ID| B_Layer
    
    %% Silver Processing Line
    B_Layer --->|3. Read Active Run ID batch| P_Spark
    S_Layer --->|4. Read Existing State| P_Spark
    P_Spark --->|5. Stateful Primary Key Upsert| S_Layer
    
    %% Gold Processing Line
    S_Layer --->|6. Load Refined State| P_Spark
    P_Spark --->|7. Star-Schema Joins + Financial Audit| G_Layer
    OLAP <--->|8. Read/Write Low-Watermark Metadata| P_Spark

    classDef container fill:#232F3E,stroke:#3F4F5F,stroke-width:2px,color:#fff;
    classDef storage fill:#115577,stroke:#0B3C5D,stroke-width:2px,color:#fff;
    class OLTP,OLAP,A_Meta storage;
    class A_Web,A_Sch,P_Spark container;

```


# 1. Project Overview

### 1.1 Introduction

The **`Ecommerce_data_pipeline`** is a production-grade, fully containerized data platform that implements a localized **Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold). Orchestrated by **Apache Airflow**, the pipeline ingests transactional data from an operational source system, executes stateful data enrichment and incremental merging via **PySpark**, and exposes highly optimized analytical assets for downstream business intelligence consumption.

The entire framework is isolated within a dedicated virtual network using **Docker Compose**, eliminating local environmental dependencies and ensuring consistent, idempotent deployments across staging and production environments.

---

### 1.2 Project Objective

The primary engineering objective of this project is to shift from a legacy, destructive full-load framework to an efficient, low-overhead **Incremental Ingestion (Change Data Capture)** mechanism. The pipeline architecture is designed to:

* Automatically capture source mutations across e-commerce core tables (`source_customers`, `source_products`, and `source_orders`) utilizing high-watermark timestamp tracking.
* Isolate, audit, and deduplicate concurrent data batches without risking data loss or state contamination.
* Compute optimized analytical aggregations that power business dashboards while maximizing infrastructure resource utilization.

---

### 1.3 Business Context & Analytical Impact

Modern e-commerce platforms generate massive volumes of transactional logs daily. Traditional data systems frequently experience performance degradation because they run daily full-table overwrites, which heavily strain transactional databases and waste storage bandwidth.

By executing micro-batch incremental workloads, this project minimizes compute footprints on operational infrastructure. At the destination tier, it builds a star-schema analytical warehouse layer that surfaces vital corporate performance health metrics:

$$
\mathrm{Total\ Revenue} = \sum(\mathrm{total\_amount})
$$

$$
\mathrm{Total\ Orders} = \mathrm{Count}(\mathrm{order\_id})
$$

Downstream analytics users (such as BI Engineers and Product Managers) can evaluate these KPIs with near-zero query lag, eliminating the technical friction usually caused by parsing raw, un-indexed backend database records.

---

### 1.4 Expected Outcome & Target Stakeholders

The end state of the automated pipeline is a structured, optimized physical data lake layout.

* **Data Storage Assets:** Raw ingestion batches are cleanly isolated by execution tokens, transformed into structurally verified historical tables, and stored as highly compressed, Hive-partitioned Parquet files organized physically by `order_date=YYYY-MM-DD`.
* **Target Audience:** The direct beneficiaries include **Data Analysts** requiring low-latency access to pre-aggregated datasets, **Data Engineers** seeking a modular blueprint for stateful change capture, and **Business Leaders** tracking daily revenue and velocity fluctuations.

---

### 1.5 Core System Scope

The boundaries of the platform are explicitly defined to enforce architectural decoupling:

| Component | In-Scope Operational Boundary | Out-of-Scope System Boundary |
| --- | --- | --- |
| **Ingestion** | Micro-batch extraction from relational PostgreSQL OLTP engines using `updated_at` watermarks. | Real-time event streaming via tools like Apache Kafka or AWS Kinesis. |
| **Processing** | In-memory distributed data cleansing, schema validation, stateful deduplication, and aggregation via PySpark. | Complex, long-term machine learning model training or real-time predictive inferencing. |
| **Orchestration** | End-to-end task scheduling, pipeline run-id propagation, dependency enforcement, and failure retries through Airflow. | Advanced multi-tenant corporate security access routing or external identity management integration (OIDC/SAML). |
| **Serving Layer** | Local physical data lake files formatted with explicit partitioning structures ready for BI engine connectivity. | Direct generation, styling, or rendering of public-facing front-end data visualizations and reporting applications. |


---

