from prefect import task, flow,context
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from pyspark.sql.functions import when
from DataIngest import DataIngest
from ml import ML
from AnalyticEngine import AnalyticEngine

dataIngest = DataIngest()
ml = ML()
analytics = AnalyticEngine()

read_data_task = task(dataIngest.read_data_from_s3)
clean_data_task = task(dataIngest.clean_up_data)
save_to_mongo = task(dataIngest.store_data_in_mongo)
ml_train = task(ml.train)
analytics_task = task(analytics.performStoreAnalysis)

@flow
def runpipeline():
    # df = read_data_task('s3a://healthcarebigdata/')
    df = read_data_task('/Users/ratikvig/Downloads/')

    cleaned_df = clean_data_task()

    save_to_mongo(cleaned_df)
    analytics_task()

    ml_train(cleaned_df)

if __name__ == '__main__':
    result = runpipeline()