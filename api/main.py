from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("CrimeAPI").getOrCreate()

app = FastAPI()

MODEL_FOLDER = "/home/iryandae/kafka_2.13-3.7.0/project/models/"

class Location(BaseModel):
    lat: float
    lon: float
    model: int

@app.post("/predict_cluster/")
def predict_cluster(data: Location):
    try:
        model_path = os.path.join(MODEL_FOLDER, f"kmeans_model_batch_{data.model}")
        if not os.path.exists(model_path):
            return {"error": f"Model batch {data.model} not found"}

        model = KMeansModel.load(model_path)

        df = spark.createDataFrame([[data.lat, data.lon]], ["LAT", "LON"])
        assembler = VectorAssembler(inputCols=["LAT", "LON"], outputCol="features")
        df_vec = assembler.transform(df)

        prediction = model.transform(df_vec).collect()[0]["prediction"]
        return {"cluster": int(prediction)}
    
    except Exception as e:
        return {"error": str(e)}
