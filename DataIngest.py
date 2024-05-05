from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from prefect import task, flow, get_run_logger
from pymongo import MongoClient
import boto3
from pyspark.sql.functions import col, count, when, sum

conf = (
        SparkConf()
        .setAppName("healthcare_datapipeline") 
        .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.s3a.access.key", "AKIAQUQGRZJUX5J264RH")
        .set("spark.hadoop.fs.s3a.secret.key", "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")
        .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/healthcare.cleaned_data") \
        .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/healthcare.cleaned_data") \
        .set("spark.sql.shuffle.partitions", "4") 
        .setMaster("local[*]") 
)

class DataIngest:

    def __init__(self):
        self.df = None
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def apply_rules(self, column, rules):
        expression = when(self.df[column].isNull(), self.df[column])  
        for old, new in rules:
            expression = expression.when(self.df[column] == old, new)
        return expression.otherwise(self.df[column])
    
    def read_data_from_s3(self, s3_uri):
        session = boto3.Session(
        aws_access_key_id='AKIAQUQGRZJUX5J264RH',
        aws_secret_access_key='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',
        region_name='us-east-2'
        )
        s3 = session.client('s3')

        response = s3.list_objects_v2(Bucket='healthcarebigdata', Prefix='')
        files = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.csv')]
        # files = ['2017.csv', '2019.csv', '2021.csv']
        dataFrames = []

        for file in files:
            tdf = self.spark.read.csv(s3_uri+file, header=True, inferSchema=True)
            tdf = tdf.select('DIABETE3', '_BMI5', '_RFBMI5', '_RFHYPE5', 
                              'TOLDHI2', '_CHOLCHK', 'CHCKIDNY', 'SMOKE100',
                              '_RFDRHV5', 'CVDSTRK3', '_MICHD', '_TOTINDA',
                              '_FRTLT1', '_VEGLT1', 'GENHLTH', 'PHYSHLTH', 'MENTHLTH',
                              'DIFFWALK', 'HLTHPLN1', 'MEDCOST', 'CHECKUP1',
                              'SEX', '_AGEG5YR', 'EDUCA', 'INCOME2')

            dataFrames.append(tdf)

        final_df = dataFrames[0]

        for index, df in enumerate(dataFrames):
            if(index == 0): continue
            final_df = final_df.union(df)

        self.df = final_df        

        self.df = self.df.na.drop()
        return self.df
    
    def clean_up_data(self):

        self.df = self.df.withColumn(
        'DIABETE3',
        when(self.df['DIABETE3'] == 2, 0)
        .when(self.df['DIABETE3'] == 3, 0)
        .when(self.df['DIABETE3'] == 1, 1)
        .when(self.df['DIABETE3'] == 4, 1)
        .otherwise(self.df['DIABETE3'])
        )
        self.df = self.df[self.df.DIABETE3 != 7]
        self.df = self.df[self.df.DIABETE3 != 9]

        self.df = self.df.withColumn(
            '_RFBMI5',
            when(self.df['_RFBMI5'] == 1, 0)
            .when(self.df['_RFBMI5'] == 2, 1)
            .otherwise(self.df['_RFHYPE5'])
        )
        self.df = self.df[self.df._RFBMI5 != 9]



        self.df = self.df.withColumn(
        '_RFHYPE5',
        when(self.df['_RFHYPE5'] == 1, 0)
        .when(self.df['_RFHYPE5'] == 2, 1)
        .otherwise(self.df['_RFHYPE5'])
        )
        self.df = self.df[self.df._RFHYPE5 != 9]

        self.df = self.df.withColumn(
        'TOLDHI2',
        when(self.df['TOLDHI2'] == 2, 0)
        .otherwise(self.df['TOLDHI2'])
        )
        self.df = self.df[self.df.TOLDHI2 != 7]
        self.df = self.df[self.df.TOLDHI2 != 9]

        self.df = self.df.withColumn(
        '_CHOLCHK',
        when(self.df['_CHOLCHK'] == 2, 0)
        .when(self.df['_CHOLCHK'] == 3, 0)
        .otherwise(self.df['_CHOLCHK'])
        )
        self.df = self.df[self.df._CHOLCHK != 9]

        self.df = self.df.withColumn(
        'CHCKIDNY',
        when(self.df['CHCKIDNY'] == 2, 0)
        .otherwise(self.df['CHCKIDNY'])
        )
        self.df = self.df[self.df.CHCKIDNY != 7]
        self.df = self.df[self.df.CHCKIDNY != 9]

        self.df = self.df.withColumn(
        'SMOKE100',
        when(self.df['SMOKE100'] == 2, 0)
        .otherwise(self.df['SMOKE100'])
        )
        self.df = self.df[self.df.SMOKE100 != 7]
        self.df = self.df[self.df.SMOKE100 != 9]

        self.df = self.df.withColumn(
        '_RFDRHV5',
        when(self.df['_RFDRHV5'] == 1, 0)
        .when(self.df['_RFDRHV5'] == 2, 1)
        .otherwise(self.df['_RFDRHV5'])
        )
        self.df = self.df[self.df._RFDRHV5 != 9]

    
        self.df = self.df.withColumn(
        'CVDSTRK3',
        when(self.df['CVDSTRK3'] == 2, 0)
        .otherwise(self.df['CVDSTRK3'])
        )
        self.df = self.df[self.df.CVDSTRK3 != 7]
        self.df = self.df[self.df.CVDSTRK3 != 9]

        self.df = self.df.withColumn(
        '_TOTINDA',
        when(self.df['_TOTINDA'] == 2, 0)
        .otherwise(self.df['_TOTINDA'])
        )
        self.df = self.df[self.df._TOTINDA != 9]

        self.df = self.df.withColumn(
        '_FRTLT1',
        when(self.df['_FRTLT1'] == 2, 0)
        .otherwise(self.df['_FRTLT1'])
        )
        self.df = self.df[self.df._FRTLT1 != 9]

        self.df = self.df.withColumn(
        '_VEGLT1',
        when(self.df['_VEGLT1'] == 2, 0)
        .otherwise(self.df['_VEGLT1'])
        )
        self.df = self.df[self.df._VEGLT1 != 9]

        self.df = self.df[self.df.GENHLTH != 7]
        self.df = self.df[self.df.GENHLTH != 9]

        self.df = self.df.withColumn(
        'PHYSHLTH',
        when(self.df['PHYSHLTH'] == 88, 0)
        .otherwise(self.df['PHYSHLTH'])
        )
        self.df = self.df[self.df.PHYSHLTH != 77]
        self.df = self.df[self.df.PHYSHLTH != 99]

        self.df = self.df.withColumn(
        'MENTHLTH',
        when(self.df['MENTHLTH'] == 88, 0)
        .otherwise(self.df['MENTHLTH'])
        )
        self.df = self.df[self.df.MENTHLTH != 77]
        self.df = self.df[self.df.MENTHLTH != 99]

        self.df = self.df.withColumn(
        'DIFFWALK',
        when(self.df['DIFFWALK'] == 2, 0)
        .otherwise(self.df['DIFFWALK'])
        )
        self.df = self.df[self.df.DIFFWALK != 7]
        self.df = self.df[self.df.DIFFWALK != 9]

        self.df = self.df.withColumn(
        'HLTHPLN1',
        when(self.df['HLTHPLN1'] == 2, 0)
        .otherwise(self.df['HLTHPLN1'])
        )
        self.df = self.df[self.df.HLTHPLN1 != 7]
        self.df = self.df[self.df.HLTHPLN1 != 9]

        self.df = self.df.withColumn(
        'MEDCOST',
        when(self.df['MEDCOST'] == 2, 0)
        .otherwise(self.df['MEDCOST'])
        )
        self.df = self.df[self.df.MEDCOST != 7]
        self.df = self.df[self.df.MEDCOST != 9]

        self.df = self.df.withColumn(
        'CHECKUP1',
        when(self.df['CHECKUP1'] == 8, 0)
        .otherwise(self.df['CHECKUP1'])
        )
        self.df = self.df[self.df.CHECKUP1 != 9]
        
    
        self.df = self.df.withColumn(
        'SEX',
        when(self.df['SEX'] == 2, 0)
        .otherwise(self.df['SEX'])
        )
        self.df = self.df[self.df.SEX != 9]


        self.df = self.df[self.df._AGEG5YR != 14]

        self.df = self.df[self.df.EDUCA != 9]

        self.df = self.df[self.df.INCOME2 != 77]
        self.df = self.df[self.df.INCOME2 != 99]

        return self.df.toPandas()


    def store_data_in_mongo(self, data):
        try:
            mongo_client = MongoClient('mongodb://localhost:27017/')
            database = mongo_client['healthcare']
            collection = database['cleaned_data']
            data = data.to_dict(orient='records')
            collection.insert_many(data)
        except Exception as e:
            print("An error occurred:")

    


