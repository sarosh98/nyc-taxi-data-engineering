# NYC Taxi Data Engineering Platform

An end-to-end data engineering platform built using Microsoft Fabric, Azure Synapse, Dataflow Gen2, Git and medallion architecture.

## Project Overview

This project simulates a production-grade transportation data platform using NYC Taxi & Limousine Commission trip data.

The platform ingests raw transportation data, applies data quality and transformation rules, builds curated analytical datasets, and exposes the resulting data for analytics and reporting.

The project demonstrates modern data engineering practices including:

* Medallion architecture
* Batch and incremental data ingestion
* Dataflow Gen2
* Fabric Lakehouse
* PySpark
* SQL
* Fabric Warehouse
* Azure Synapse integration
* Data quality and validation
* Pipeline orchestration
* Git-based development
* CI/CD
* Dimensional data modeling
* Monitoring and failure handling

## Architecture

The platform follows a Bronze → Silver → Gold architecture.

```text
NYC TLC Data
     |
     v
Fabric Pipeline
     |
     v
Dataflow Gen2
     |
     v
Bronze Lakehouse
     |
     v
Silver Lakehouse
     |
     +---- Data Quality / Validation
     |
     v
Spark / SQL Transformations
     |
     v
Gold Warehouse
     |
     +------------+
     |            |
     v            v
 Power BI      Azure Synapse
```

## Technology Stack

| Layer                 | Technology                           |
| --------------------- | ------------------------------------ |
| Source                | NYC TLC Trip Record Data             |
| Storage               | Microsoft OneLake                    |
| Bronze                | Fabric Lakehouse                     |
| Silver                | Fabric Lakehouse                     |
| Transformation        | Dataflow Gen2, PySpark, SQL          |
| Gold                  | Fabric Warehouse                     |
| Orchestration         | Fabric Data Pipelines                |
| Analytics Integration | Azure Synapse                        |
| Version Control       | GitHub                               |
| CI/CD                 | Git + Fabric deployment capabilities |
| Visualization         | Power BI                             |

## Engineering Objectives

The project focuses on demonstrating how and why different data engineering tools are selected rather than simply using every available technology.

Key architectural decisions will be documented throughout the project.

## Repository Structure

The repository will evolve as the platform is developed.

```text
nyc-taxi-data-engineering/
│
├── README.md
├── architecture/
├── notebooks/
├── sql/
├── pipelines/
├── dataflows/
├── tests/
└── docs/
```

## Data Source

NYC Taxi & Limousine Commission:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Project Status

* [x] Fabric development workspace
* [x] GitHub repository
* [x] Fabric Git integration
* [x] Bronze Lakehouse
* [ ] Source data ingestion
* [ ] Bronze ingestion pipeline
* [ ] Silver transformation
* [ ] Data quality framework
* [ ] Gold dimensional model
* [ ] Incremental processing
* [ ] Azure Synapse integration
* [ ] CI/CD
* [ ] Power BI analytics

## Architecture Decision Records

Architectural decisions will be documented as the project progresses, including:

* Why Fabric Lakehouse instead of Warehouse for Bronze/Silver
* Why Dataflow Gen2 instead of Spark for selected transformations
* Why Spark is introduced for complex transformations
* Why Fabric Warehouse is used for the Gold layer
* Why Azure Synapse is retained as an integration point
* Why Git-based development is used
* Why incremental processing is preferred over full reloads
