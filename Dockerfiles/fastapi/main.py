from fastapi import FastAPI, HTTPException
from schemas import CoverType
import pandas as pd
import os
import mlflow
from mlflow.tracking import MlflowClient


# Load model
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://s3:9000"
os.environ['AWS_ACCESS_KEY_ID'] = 'mlflows3'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'mlflows3'

mlflow.set_tracking_uri("http://mlflow-webserver:5000")

model_name = "Random_Forest_Model"
model_prod_uri = "models:/{model_name}/production".format(model_name=model_name)
model = mlflow.pyfunc.load_model(model_uri=model_prod_uri)

client = MlflowClient()
model_version = client.get_latest_versions(model_name, stages=["Production"])[0]


# Initialize FastAPI
app = FastAPI()


# Define endpoint for root URL
@app.get("/")
async def root():
    return {"Nombre del modelo": model_version.name, "Versión en producción": model_version.version}

# Define endpoint for prediction
@app.post("/predict")
async def predict_readmitted(data: CoverType):
    # Perform prediction
    prediction = model.predict(
        [
            [
                data.brokered_by,
                data.bed,
                data.bath,
                data.acre_lot,
                data.street,
                data.zip_code,
                data.house_size,
                data.prev_sold_date,
                data.years_since_sold
            ]
        ]
    )
    # Convertir el resultado de la predicción a un diccionario
    prediction_dict = {
        "prediction": str(prediction[0])
    }  # Convertir la predicción a una cadena
    return prediction_dict
