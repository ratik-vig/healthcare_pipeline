# Data Ingestion Pipeline and Machine Learning using PySpark

This project implements a data ingestion pipeline using PySpark, designed to efficiently process, transform and store large volumes of healthcare data. 
The pipeline leverages PySpark's distributed computing capabilities to handle big data scenarios, ensuring scalability and performance.
Fetches .csv files from s3 buckets, cleans and stores in mongoDB. Finally, trains deep learning models on the cleaned data.

## Features

- **Scalable Data Processing**: Utilize Spark's distributed architecture to handle large datasets.
- **Data Transformation**: Includes common transformations required for data preprocessing.

## Prerequisites

Before you can run this pipeline, you need to ensure that your environment is set up with the following:

- Python 
- Apache Spark 
- Java 8

## Installation

Follow these steps to set up your environment and run the pipeline:

### 1. Clone the Repository

```bash
git clone https://github.com/ratik-vig/healthcare_pipeline.git
cd healthcare_pipeline
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

Finally, go to pipeline.py file and run
