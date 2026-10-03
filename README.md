#  Crypto Market Data Engineering Pipeline

An end-to-end Python data engineering project that ingests live cryptocurrency market data from REST APIs, performs data-quality validation, routes invalid records to a Dead Letter Queue (DLQ), stores validated data in a partitioned data lake, performs SQL analytics using SQLite, and visualizes the results through an interactive Flask dashboard.

---

##  Project Overview

This project demonstrates a complete data pipeline starting from live API ingestion and ending with analytical dashboards.

The pipeline performs the following steps:

1. Fetch live cryptocurrency market data from the CoinGecko API.
2. Validate incoming records for required fields and data types.
3. Route invalid records to a Dead Letter Queue (DLQ).
4. Store validated records in a date-partitioned data lake.
5. Load validated records into SQLite.
6. Execute analytical SQL queries.
7. Present the results through a Flask web dashboard.
8. Provide interactive USD/INR currency conversion.
9. Visualize market data using interactive charts.

---
