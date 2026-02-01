# Participant Inventory

A production-grade participant inventory platform for research organisations.

This project delivers a scalable, industry-style system for managing participant-level research data across heterogeneous sources. It is designed to support real operational workflows, including structured data ingestion, schema-governed validation, and fast cohort discovery, rather than ad hoc or exploratory analysis.

The platform is built with a forward-looking architecture that supports incremental data growth, auditability, and future integration of AI-assisted capabilities such as research feasibility assessment and automated MRI quality control.

## Problem Statement

Research organisations routinely manage participant data across multiple independent systems and pipelines, such as demographics, clinical assessments, imaging outputs, and derived biomarkers. In practice, these data are often stored in disconnected spreadsheets or siloed databases, creating operational risk and limiting scalability.

Common challenges include:
- No single, authoritative view of participant-level data
- Manual and error-prone data merging across sources
- Limited visibility into data completeness and cohort availability
- Slow turnaround when assessing feasibility for new projects or analyses
- Systems that do not scale as variables and participants increase

These issues introduce friction into research operations, slow decision-making, and increase the likelihood of data integrity errors. This project addresses these challenges by providing a unified, schema-driven participant inventory designed for production use in data-intensive research environments.

## Core Principles

The platform is designed around a set of pragmatic principles informed by real-world data operations rather than exploratory analysis.

### 1. Schema as a Contract
All data ingestion is governed by an explicit, versioned schema that defines expected variables, types, and constraints. This ensures consistency across uploads, prevents silent data corruption, and enables reliable downstream querying.

### 2. Production-First Data Integrity
The system prioritises validation, auditability, and traceability over convenience. Every upload is validated, tracked, and attributable, with clear visibility into data provenance and ingestion history.

### 3. Incremental and Heterogeneous Data Ingestion
Data are expected to arrive in stages, across independent pipelines, and at different times. The architecture supports partial uploads and safe merging without requiring complete datasets upfront.

### 4. Scalable Variable Management
The system is designed to support thousands of participant-level variables without relying on brittle, wide-table designs. This enables growth in both cohort size and feature dimensionality.

### 5. Fast and Transparent Cohort Discovery
Cohort queries are designed to be explicit, inspectable, and performant. Users should be able to understand not only *how many* participants meet criteria, but *why*.


### 6. Forward-Compatible Architecture
Core infrastructure decisions are made to support future extensions, including AI-assisted cohort feasibility analysis, derived variable tracking, and automated quality control, without reworking foundational components.

## MVP Scope

The initial MVP focuses on establishing a reliable foundation for participant-level data ingestion and cohort discovery. The goal of this phase is to deliver a production-ready inventory backbone before introducing advanced analytics or AI-driven functionality.

### MVP Objectives
- Establish a single source of truth for participant-level data
- Support ingestion of structured data from multiple independent sources
- Enforce schema-driven validation at ingestion time
- Enable basic cohort discovery through explicit, structured filters
- Provide visibility into data completeness and coverage

### MVP Capabilities
The first MVP will deliver:
- Participant identity management with support for multi-source data
- Upload of structured datasets (e.g. CSV, Excel) via a web interface
- Automated validation against a predefined schema
- Safe merging of partial datasets into a unified inventory
- Basic cohort queries using structured filters (e.g. demographic and clinical variables)
- Simple summary outputs such as participant counts and data availability

### Out of Scope for MVP
The following capabilities are intentionally deferred:
- Natural language querying
- AI-assisted feasibility analysis
- Automated MRI quality control
- Advanced visualisation and reporting
- Role-based access control and permissions

These features are planned for later stages once the core inventory and ingestion workflows are stable.

## High-Level Architecture

The platform is designed as a modular system with clear separation between user-facing interfaces, core business logic, and persistent storage. This separation enables independent evolution of components while maintaining a stable system of record.

### Frontend
A web-based interface provides controlled access to core workflows, including data upload, validation feedback, and cohort exploration. The frontend is intentionally thin, delegating validation and data logic to backend components.

### Core Application Logic
The core application layer implements ingestion pipelines, schema validation, participant reconciliation, and query construction. Business rules are centralised to ensure consistent behaviour across interfaces and deployment environments.

### Data Store
A relational database serves as the system of record for participant identities, variables, and values. The data model is designed to support incremental ingestion, high-dimensional feature sets, and efficient cohort queries while preserving auditability.

### Schema Registry
An explicit schema registry defines expected datasets, variables, data types, and constraints. This registry acts as a contract between data producers and the system, enabling predictable ingestion and reducing operational risk.

### Extensibility Layer
The architecture is designed to accommodate future extensions such as AI-assisted cohort feasibility analysis, derived variable tracking, and automated quality control pipelines without modifying core ingestion or storage logic.

## Technology Stack

The technology stack is selected to balance rapid iteration with production-readiness, prioritising reliability, maintainability, and extensibility.

- **Python**  
  Core application language, chosen for its strong ecosystem in data engineering, validation, and applied machine learning.

- **Streamlit**  
  Provides a lightweight web interface for controlled data upload and cohort exploration, enabling fast iteration while maintaining a clean separation from core logic.

- **PostgreSQL**  
  Serves as the primary system of record for participant identities, variables, and values, offering strong consistency guarantees and robust query performance.

- **SQLAlchemy**  
  Manages database interactions and schema definitions while keeping business logic database-agnostic.

- **Pydantic / Pandera**  
  Enforces schema-driven validation and type safety at ingestion time, reducing the risk of silent data errors.

- **Docker**  
  Supports reproducible local development and deployment across environments.

- **GitHub**  
  Provides version control, issue tracking, and the foundation for future CI/CD workflows.

The stack is intentionally conservative and widely adopted, ensuring long-term support and ease of onboarding for new contributors.

## Repository Structure

The repository is organised to clearly separate user interfaces, core business logic, and persistence layers. This structure is intended to support long-term maintainability and collaborative development.

```text
.
├── src/                  # Application source code
│   ├── app/              # Streamlit UI components
│   ├── core/             # Ingestion, validation, and query logic
│   └── db/               # Database models and migrations
├── schema/               # Dataset and variable definitions (schema registry)
├── docs/                 # Design notes, ingestion rules, and architecture decisions
├── tests/                # Automated tests
├── pyproject.toml        # Project configuration and dependencies
└── README.md

```

## Development Status

This project is in active development.

The current focus is on designing and implementing the core ingestion and inventory infrastructure, including schema definition, participant identity management, and foundational data models. Interfaces and workflows may evolve as the system is exercised against real-world use cases.

Breaking changes may occur during early development stages. Stability guarantees will be introduced once the core data model and ingestion pipelines have been validated in practice.

## Context

This project is being developed as part of applied research infrastructure work in collaboration with a research lab at the University of Melbourne.

The system is designed to be institution-agnostic and suitable for reuse across research organisations, with a focus on production-grade data management rather than project-specific or exploratory tooling.

## License

This project is released under the MIT License.  
See the `LICENSE` file for details.

## Contact and Updates

For project updates, design discussions, and related writing on research infrastructure and applied AI, you can follow or contact me via the channels below:

- **LinkedIn:** https://www.linkedin.com/in/oscar-alateras-411372293  
- **Medium:** https://medium.com/@oscaralateras70  
- **Email:** oscaralateras70@gmail.com

These channels will be used to share development updates, architectural decisions, and broader reflections related to this project.





