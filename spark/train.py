from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import os
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

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
# TRAINING RANDOM FOREST CLASSIFIER
print("\n🌲 Starting training Random Forest Classifier models")

for idx, batch_file in enumerate(batch_files):
    print(f"📂 Processing (RF): {batch_file}")
    
    df = spark.read.csv(batch_file, header=True, inferSchema=True)
    df = df.select("LAT", "LON", "Victim.Age", "Victim.Sex", "Weapon.Desc", "Crime.Code", "Status.Code", "Severity").dropna()

    # Encoding categorical columns (example: Victim.Sex, Weapon.Desc)
    from pyspark.ml.feature import StringIndexer
    sex_indexer = StringIndexer(inputCol="Victim.Sex", outputCol="Sex_Index")
    weapon_indexer = StringIndexer(inputCol="Weapon.Desc", outputCol="Weapon_Index")
    
    assembler = VectorAssembler(
        inputCols=["LAT", "LON", "Victim.Age", "Sex_Index", "Weapon_Index", "Crime.Code", "Status.Code"],
        outputCol="features"
    )
    
    rf = RandomForestClassifier(featuresCol="features", labelCol="Severity", numTrees=20)
    
    pipeline = Pipeline(stages=[sex_indexer, weapon_indexer, assembler, rf])
    model = pipeline.fit(df)
    
    rf_model_path = os.path.join(output_folder, f"rf_model_batch_{idx+1}")
    model.save(rf_model_path)
    print(f"✅ RF Model saved to {rf_model_path}")

spark.stop()
