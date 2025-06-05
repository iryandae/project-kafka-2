from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("CrimeAPI").getOrCreate()
app = FastAPI()

MODEL_FOLDER = "/home/iryandae/kafka_2.13-3.7.0/project/models/"

# ========== KMEANS ==========
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

# ========== RANDOM FOREST ==========
class SeverityFeatures(BaseModel):
    lat: float
    lon: float
    age: float
    sex: str
    premis_desc: str
    status: str
    model: int

@app.post("/predict-severity/")
def predict_severity(data: SeverityFeatures):
    try:
        model_path = os.path.join(MODEL_FOLDER, f"rf_model_batch_{data.model}")
        if not os.path.exists(model_path):
            return {"error": f"Model batch {data.model} not found"}

        model = PipelineModel.load(model_path)
        df = spark.createDataFrame([[
            data.lat, data.lon, data.age,
            data.sex, data.premis_desc, data.status
        ]], ["LAT", "LON", "Vict Age", "Vict Sex", "Premis Desc", "Status"])
        prediction = model.transform(df).collect()[0]["prediction"]
        return {"severity_prediction": int(prediction)}
    except Exception as e:
        return {"error": str(e)}

# ========== DECISION TREE ==========
class CrimeTypeFeatures(BaseModel):
    lat: float
    lon: float
    age: float
    sex: str
    premis_desc: str
    status: str
    model: int

@app.post("/predict-crime-type/")
def predict_crime_type(data: CrimeTypeFeatures):
    try:
        model_path = os.path.join(MODEL_FOLDER, f"dt_model_batch_{data.model}")
        if not os.path.exists(model_path):
            return {"error": f"Model batch {data.model} not found"}

        model = PipelineModel.load(model_path)
        df = spark.createDataFrame([[
            data.lat, data.lon, data.age,
            data.sex, data.premis_desc, data.status
        ]], ["LAT", "LON", "Vict Age", "Vict Sex", "Premis Desc", "Status"])
        prediction = model.transform(df).collect()[0]["prediction"]
        return {"predicted_crime_code": int(prediction)}
    except Exception as e:
        return {"error": str(e)}
