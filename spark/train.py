from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml import Pipeline
import os

spark = SparkSession.builder \
    .appName("Crime Training Models") \
    .getOrCreate()

batch_folder = "/home/iryandae/kafka_2.13-3.7.0/project/data/batch/"
output_folder = "/home/iryandae/kafka_2.13-3.7.0/project/models/"

batch_files = sorted([
    os.path.join(batch_folder, f)
    for f in os.listdir(batch_folder)
    if f.endswith(".csv")
])

# ==== KMEANS ====
print("\n📌 Starting KMeans Training")
for idx, batch_file in enumerate(batch_files):
    print(f"📂 Processing (KMeans): {batch_file}")
    df = spark.read.csv(batch_file, header=True, inferSchema=True)
    df = df.select("LAT", "LON").dropna()

    assembler = VectorAssembler(inputCols=["LAT", "LON"], outputCol="features")
    df_vec = assembler.transform(df)

    kmeans = KMeans(k=5, seed=1)
    model = kmeans.fit(df_vec)

    model_path = os.path.join(output_folder, f"kmeans_model_batch_{idx+1}")
    model.save(model_path)
    print(f"✅ KMeans model saved to {model_path}")

# ==== RANDOM FOREST (predict severity) ====
print("\n🌲 Starting Random Forest (Severity)")
for idx, batch_file in enumerate(batch_files):
    print(f"📂 Processing (RF): {batch_file}")
    df = spark.read.csv(batch_file, header=True, inferSchema=True)
    df = df.select("LAT", "LON", "Vict Age", "Vict Sex", "Premis Desc", "Status", "Part 1-2").dropna()

    sex_indexer = StringIndexer(inputCol="Vict Sex", outputCol="Sex_Index")
    premis_indexer = StringIndexer(inputCol="Premis Desc", outputCol="Premis_Index")
    status_indexer = StringIndexer(inputCol="Status", outputCol="Status_Index")

    assembler = VectorAssembler(
        inputCols=["LAT", "LON", "Vict Age", "Sex_Index", "Premis_Index", "Status_Index"],
        outputCol="features"
    )

    rf = RandomForestClassifier(featuresCol="features", labelCol="Part 1-2", numTrees=20)
    pipeline = Pipeline(stages=[sex_indexer, premis_indexer, status_indexer, assembler, rf])
    model = pipeline.fit(df)

    rf_model_path = os.path.join(output_folder, f"rf_model_batch_{idx+1}")
    model.save(rf_model_path)
    print(f"✅ RF model saved to {rf_model_path}")

# ==== DECISION TREE (predict crime type) ====
print("\n🌳 Starting Decision Tree (Crime Type)")
for idx, batch_file in enumerate(batch_files):
    print(f"📂 Processing (DT): {batch_file}")
    df = spark.read.csv(batch_file, header=True, inferSchema=True)
    df = df.select("LAT", "LON", "Vict Age", "Vict Sex", "Premis Desc", "Status", "Crm Cd").dropna()

    sex_indexer = StringIndexer(inputCol="Vict Sex", outputCol="Sex_Index")
    premis_indexer = StringIndexer(inputCol="Premis Desc", outputCol="Premis_Index")
    status_indexer = StringIndexer(inputCol="Status", outputCol="Status_Index")

    assembler = VectorAssembler(
        inputCols=["LAT", "LON", "Vict Age", "Sex_Index", "Premis_Index", "Status_Index"],
        outputCol="features"
    )

    dt = DecisionTreeClassifier(featuresCol="features", labelCol="Crm Cd")
    pipeline = Pipeline(stages=[sex_indexer, premis_indexer, status_indexer, assembler, dt])
    model = pipeline.fit(df)

    dt_model_path = os.path.join(output_folder, f"dt_model_batch_{idx+1}")
    model.save(dt_model_path)
    print(f"✅ DT model saved to {dt_model_path}")

spark.stop()
