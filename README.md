# News Classification

This project aims to classify news as fake news and reliable news.

## Getting Started

These instructions will get you a copy of the project up and running on Google Cloud Platform for development and testing purposes. 

### Prerequisites

```
1. Create a Google Cloud Platform(GCP) Project;
2. Under this project, create a bucket and upload dataset;
3. Under this project, create a cluster with following configuration:
  3.1. 1 Master node with 4 vcores and 26 GB RAM;
  3.2. 5 Work nodes, each with 4 vcores and 26 GB RAM;
  3.3. VM as Debian 9, Hadoop 2.9, Spark 2.4;
  3.4. Web Components with Anaconda and Juypter notebook.
```

## Reading data from Bucket

from google.cloud import storage
pd_df = pd.read_csv('gs://<<bucket_path>>/<<bucket_file>>')

## Create Spark Session

spark = SparkSession.builder \
        .appName("fakenews") \
        .config("spark.master", "yarn") \
        .config("spark.submit.deployMode", "cluster") \
        .config("spark.driver.memory", "25g") \
        .config("spark.executor.instances", "5") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "25g") \
        .getOrCreate()
py_df = spark.createDataFrame(pd_df)

## Folders & Files

1. Old_* folders are initial versions of the programs;
2. fakenews_v2_baseline_model.ipynb - 
  2.1. models trained and tested with POA feature only from content
  2.2. models trained and tested with tf-idf feature only from content
3. fakenews_v2_enhanced_model.ipynb
  3.1. models trained and tested with POA feature only from content
  3.2. models trained and tested with tf-idf feature only from content
  
## List of Models

1. LogisticRegression
2. DecisionTreeClassifier
3. RandomForestClassifier
4. GBTClassifier
5. NaiveBayes

## Upload results and files to Bucket

bucket_root_path = <<bucket_name>>
project_data_folder = <<bucket_path>>

def upload_files(files):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_root_path)
    for file in files:
        butcketFile = project_data_folder + file
        blob = bucket.blob(butcketFile)
        blob.upload_from_filename(file)
        print("Upload from local {0} to {1}".format(file, butcketFile))

upload_files([<<file_name>>])
