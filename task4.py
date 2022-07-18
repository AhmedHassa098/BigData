from pyspark.ml import Pipeline
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
#task4
#sparkmlib Code

#positive label and a dense feature vector.
positiveValues = LabeledPoint(1.0, [2.0, 1.0, 4.0])
print(positiveValues)
#  negative label and a sparse feature vector.
negativeValues = LabeledPoint(0.0, SparseVector(3, [0, 2], [2.0, 4.0]))
print(negativeValues)
#Ml Dataframe loading
df1 = spark.read.format("csv").option("header", "true").load(
    "dbfs:/FileStore/shared_uploads/ahmedhassan0057@gmail.com/HRDataset_v14-8.csv")

#pipe

from pyspark.ml import Pipeline
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.ml.feature import StringIndexer

# create a sample dataframe
Cars_DataFrame = spark.createDataFrame([
    (1, 'Suzuki', 'japan'),
    (2, 'Honda', 'German'),
    (3, 'Dinamo', 'japan'),
    (4, 'Bmw', 'German'),
    (5, 'mercedes', 'German')
], ['id', 'Cars', 'Company'])

Cars_DataFrame.show()
from pyspark.ml.feature import OneHotEncoder
# define stage 1 : transform the column Cars to numeric
stage_1 = StringIndexer(inputCol='Cars', outputCol='Cars_1')
# define stage 2 : transform the Company category_2 to numeric
stage_2 = StringIndexer(inputCol='Company', outputCol='Company_1')
# define stage 3 : one hot encode the numeric Company_1 column
stage_3 = OneHotEncoder(inputCols=['Company_1'], outputCols=[
                        'Company_1_Further'])

# setup the pipeline
pipeline = Pipeline(stages=[stage_1, stage_2, stage_3])

# fit the pipeline model and transform the data as defined
pipeline_model = pipeline.fit(Cars_DataFrame)
Cars_DataFrame_Advanced = pipeline_model.transform(Cars_DataFrame)

# view the transformed data
Cars_DataFrame_Advanced.show()

#task4(5)
import pandas as pd
import numpy as np
df=pd.read_csv(r"C:\Users\hp\Downloads\HRDataset_v14.csv", encoding="latin-1")
print(df)