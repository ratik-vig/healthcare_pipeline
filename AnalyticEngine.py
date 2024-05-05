
from DataIngest import DataIngest
from pyspark.sql.functions import when, count, sum, col, corr
from pyspark.sql.types import StructType, StructField, StringType, FloatType

dataIngest = DataIngest()

class AnalyticEngine:
    
    def __init__(self):
        self.df = None


    def loadDataFrame(self):
        self.df = dataIngest.spark.read.format("mongo").load()

    def performStoreAnalysis(self):
        self.loadDataFrame()

        self.df.createOrReplaceTempView("health_data")
        result = dataIngest.spark.sql("""
            SELECT DIABETE3, SEX, AVG(_BMI5) AS avg_bmi
            FROM health_data
            GROUP BY DIABETE3, SEX
            ORDER BY DIABETE3, SEX
        """)
        result.write.format("mongo").mode("overwrite").option("uri", "mongodb://127.0.0.1/healthcare.bmi_diabetes").save()

        result2 = dataIngest.spark.sql("""
            SELECT TOLDHI2, DIABETE3, SEX, 
                COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY TOLDHI2, SEX) * 100 AS percentage
            FROM health_data
            GROUP BY TOLDHI2, DIABETE3, SEX
            ORDER BY TOLDHI2, DIABETE3, SEX
        """)
        result2.write.format("mongo").mode("overwrite").option("uri", "mongodb://127.0.0.1/healthcare.cholesterol_diabetes").save()

        result3 = dataIngest.spark.sql("""
           SELECT 
                SMOKE100 AS Smoked, 
                _RFDRHV5 AS HeavyDrinker, 
                DIABETE3 AS DiabetesStatus, 
                
                COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY SMOKE100, _RFDRHV5) * 100 AS percentage
            FROM health_data
            GROUP BY SMOKE100, _RFDRHV5, DIABETE3
            ORDER BY SMOKE100, _RFDRHV5, DIABETE3
        """)

        result3.write.format("mongo").mode("overwrite").option("uri", "mongodb://127.0.0.1/healthcare.smoking_drinking").save()

        result4 = dataIngest.spark.sql("""
            WITH TotalCounts AS (
                SELECT 
                    CVDSTRK3 AS StrokeHistory, 
                    _MICHD AS CoronaryHeartDisease, 
                    COUNT(*) AS Total
                FROM health_data
                GROUP BY CVDSTRK3, _MICHD
            ),
            DiabetesCounts AS (
                SELECT 
                    CVDSTRK3 AS StrokeHistory, 
                    _MICHD AS CoronaryHeartDisease, 
                    DIABETE3 AS DiabetesStatus, 
                    COUNT(*) AS Count
                FROM health_data
                GROUP BY CVDSTRK3, _MICHD, DIABETE3
            )
            SELECT 
                a.StrokeHistory,
                a.CoronaryHeartDisease,
                a.DiabetesStatus,
                a.Count,
                (a.Count / b.Total) * 100 AS Percentage
            FROM DiabetesCounts a
            JOIN TotalCounts b 
                ON a.StrokeHistory = b.StrokeHistory 
                AND a.CoronaryHeartDisease = b.CoronaryHeartDisease
            ORDER BY a.StrokeHistory, a.CoronaryHeartDisease, a.DiabetesStatus
            """)

        result4.write.format("mongo").mode("overwrite").option("uri", "mongodb://127.0.0.1/healthcare.heart_stroke").save()

        result5 = dataIngest.spark.sql("""
            SELECT _RFHYPE5, DIABETE3, SEX, 
                COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY _RFHYPE5, SEX) * 100 AS percentage
            FROM health_data
            GROUP BY _RFHYPE5, DIABETE3, SEX
            ORDER BY _RFHYPE5, DIABETE3, SEX
        """)
        result5.write.format("mongo").mode("overwrite").option("uri", "mongodb://127.0.0.1/healthcare.highbp_diabetes").save()


        res = self.df.groupBy('GENHLTH', '_AGEG5YR').agg(
            (sum(when(col('DIABETE3') == 1, 1)) / count('*') * 100).alias('diabetes_prevalence')
        ).orderBy('GENHLTH', '_AGEG5YR')

        res.write.format("mongo").mode("overwrite").option("uri", "mongodb://127.0.0.1/healthcare.test").save()
