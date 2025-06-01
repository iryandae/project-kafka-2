from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import os

spark = SparkSession.builder \
    .appName("Crime Clustering") \
    .getOrCreate()

batch_folder = "/home/iryandae/kafka_2.13-3.7.0/project/data/batch/"
output_folder = "/home/iryandae/kafka_2.13-3.7.0/project/models/"

batch_files = sorted([
    os.path.join(batch_folder, f)
    for f in os.listdir(batch_folder)
    if f.endswith(".csv")
])

for idx, batch_file in enumerate(batch_files):
    print(f"📂 Processing: {batch_file}")
    
    df = spark.read.csv(batch_file, header=True, inferSchema=True)
    df = df.select("LAT", "LON").dropna()

    assembler = VectorAssembler(inputCols=["LAT", "LON"], outputCol="features")
    df_vec = assembler.transform(df)

    kmeans = KMeans(k=5, seed=1)
    model = kmeans.fit(df_vec)

    model_path = os.path.join(output_folder, f"kmeans_model_batch_{idx+1}")
    model.save(model_path)
    print(f"✅ Model saved to {model_path}")

spark.stop()
