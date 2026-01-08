# Airport ETL Project

## Overview
This project implements an ETL pipeline that collects flight departure data, processes it, stores it in an RDS PostgreSQL database, and exposes aggregated analytics through a web dashboard. The system is fully containerized and deployed on AWS.

**Dashboard URL:** http://airport-etl-alb-2046359646.eu-north-1.elb.amazonaws.com/

## Architecture Components

### 1. Scraper
- Scrapes departure data from Warsaw Chopin Airport website using Selenium
- Normalizes and inserts raw data into PostgreSQL
- Runs once per day as a scheduled ECS task

### 2. Analyzer
- Reads raw flight data from PostgreSQL
- Computes aggregated statistics:
  - Top destinations
  - Busiest airlines
  - Hourly traffic patterns
- Stores results in dedicated statistics tables
- Runs once per day after the scraper completes

### 3. Aggregator (Dashboard)
- Streamlit web application
- Reads precomputed statistics and raw data from PostgreSQL
- Runs continuously as an ECS Service behind an Application Load Balancer

## AWS Infrastructure

### Core Services
- **Amazon ECS (Fargate)**: Container orchestration
  - Runs scraper, analyzer, and aggregator containers
  - Scraper and analyzer execute as scheduled tasks
  - Aggregator runs as a long-lived ECS Service
- **Amazon ECR**: Docker image repository for all components
- **Amazon RDS (PostgreSQL)**: Central data store for raw and aggregated flight data
- **Application Load Balancer (ALB)**: Public access point for the Streamlit dashboard
- **Amazon EventBridge**: Triggers daily execution of scraper and analyzer via cron schedules
- **Amazon CloudWatch**: Centralized logging for all ECS tasks and services

## Data Pipeline Flow
1. **Extract**: Daily scraping of departure data from Warsaw Chopin Airport
2. **Transform**: Data normalization and aggregation
3. **Load**: Storage in PostgreSQL with precomputed statistics
4. **Visualize**: Real-time dashboard access to processed analytics

## Deployment
The system is designed for automated deployment on AWS with all components containerized and managed through ECS. Each component can be scaled independently based on workload requirements.
