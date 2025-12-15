This repository contains a collection of Python-based data spiders designed to systematically crawl public APIs, extract relevant structured information, and persist it into relational databases.

The scripts focus on:

Deterministic URL exploration

Resilient HTTP request handling with retries and backoff

Structured JSON parsing and field extraction

Idempotent database writes using upserts

Auditability and re-runnable data ingestion

These spiders can be used as standalone ingestion jobs or as building blocks for larger ETL and data engineering pipelines.

Repository showcasing data engineering fundamentals through API spiders: crawling, parsing, error handling, and reliable persistence of structured data into PostgreSQL.
