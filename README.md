# Spotify-Data-Pipeline-Project

# Overview
This project is a data engineering solution designed to extract, transform, and load (ETL) data from the Spotify API. The goal is to create an automated pipeline that ingests real-time data on the songs a user is currently playing, processes the data, and stores it in a data warehouse for analysis.

The project uses Apache Airflow for orchestration, MinIO for storage (mimicking AWS S3), PySpark for data transformation, and PostgreSQL as the final destination for the processed data. The pipeline handles raw data in JSON format, applies transformations to filter  the data, and loads the processed results into PostgreSQL for further analysis.
![diagram-export-10-20-2024-7_02_38-PM](https://github.com/user-attachments/assets/4eaa08c1-d5a4-4b83-967b-1f2c9bb9c14a)
# Purpose
The purpose of this project is to showcase a complete end-to-end data engineering pipeline. It demonstrates key skills such as data ingestion, transformation, and loading while working with APIs, a data lake, and a warehouse.

## Technologies Used
- **Spotify API**: The source of real-time user listening data.
- **Apache Airflow**: Orchestrates the ETL process by defining tasks and their execution order.
- **MinIO**: Acts as the object storage (simulating AWS S3) for raw and processed data.
- **PySpark**: Handles the transformation of raw JSON data into structured formats and performs filtering/aggregation.
- **PostgreSQL**: A relational database used as the final data warehouse.
- **Docker**: Used to containerize the project components for consistent and portable environments.
-
## Data Pipeline Tasks

### 1. Ingest Data from Spotify API
- A request is made to the Spotify API to fetch user listening data, which is returned in raw JSON format.

### 2. Store Raw Data in MinIO
- The raw JSON data is stored in MinIO object storage (as the data lake).

### 3. Transform Data
- Using PySpark, the raw JSON data is transformed into a structured format. This involves:
  - Extracting relevant fields like track name, album, artist, played_at, etc.
  - Saving the transformed data in Parquet format (optimized for analytics).

### 4. Store Processed Data in MinIO
- The transformed Parquet files are saved in a MinIO bucket, representing the processed data.

### 5. Load Data into PostgreSQL
- The Parquet data is loaded into PostgreSQL, where it is structured into tables for analysis.
