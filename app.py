from flask import Flask,jsonify,request,render_template, url_for, redirect
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient, generate_blob_sas, ContainerSasPermissions
import pandas as pd
import string, random, requests
from datetime import datetime, timedelta
from logging.config import fileConfig

app = Flask(__name__, instance_relative_config=True)
fileConfig('logging.cfg')

app.config.from_pyfile('config.py')
account = app.config['ACCOUNT']   # Azure account name
key = app.config['STORAGE_KEY']      # Azure Storage account access key
container = app.config['CONTAINER'] # Container name
blob_name = app.config['BLOB_NAME']      # Blob name
performance_file_name = app.config['PERFORMANCE_FILE_NAME']
scoring_file_name = app.config['SCORING_FILE_NAME']
connection_string = app.config['CONNECTION_STRING'] # Connection String

blob_service = BlobServiceClient.from_connection_string(conn_str=connection_string)

# using generate_blob_sas
def get_blob_url_with_blob_sas_token(blob_name):
    try:
        blob_sas_token = generate_blob_sas(
            account_name=account,
            container_name=container,
            blob_name=blob_name,
            account_key=key,
            permission=ContainerSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        blob_url_with_blob_sas_token = f"https://{account}.blob.core.windows.net/{container}/{blob_name}?{blob_sas_token}"
    except:
        app.logger.error("Blob URL could not be generated")
    return blob_url_with_blob_sas_token

def normalize(data):
    if isinstance(data, dict):
        data = {normalize(key): normalize(value) for key, value in data.items()}
    elif isinstance(data, list):
        data = [normalize(item) for item in data]
    return data

@app.route('/')
def home():
  return render_template('index.html')

@app.route('/model/<string:modelname>/<string:modelversion>/performance')
def get_model_performance(modelname,modelversion):
    full_blob_name = blob_name + modelname + "/" + modelversion + "/" + performance_file_name

    try:
        blob_url_with_sas = get_blob_url_with_blob_sas_token(full_blob_name)
    except:
        app.logger.error("Error encountered while generating blob URL {0}".format(full_blob_name))
        return ("Error encountered while generating blob URL {0}".format(full_blob_name), 404)

    try:
        # pass the blob url with sas to function `read_excel`
        df = pd.read_excel(blob_url_with_sas)
    except:
        app.logger.error("File not found for model {0}".format(modelname))
        return("File not found for model {0}".format(modelname), 404)

    try:
        data = df.to_dict(orient='records')
        data = normalize(data)
    except:
        app.logger.error("Error reading file for model {0}".format(modelname))
        return("Error reading file for model {0}".format(modelname), 400)
    return jsonify(data)

@app.route('/model/<string:modelname>/<string:modelversion>/score/<int:unique_id>')
def get_model_score(modelname, modelversion, unique_id):
    full_blob_name = blob_name + modelname + "/" + modelversion + "/" + scoring_file_name

    try:
        blob_url_with_sas = get_blob_url_with_blob_sas_token(full_blob_name)
    except:
        app.logger.error("Error encountered while generating blob URL {0}".format(full_blob_name))
        return ("Error encountered while generating blob URL {0}".format(full_blob_name), 404)

    try:
        # pass the blob url with sas to function `read_excel`
        df = pd.read_excel(blob_url_with_sas)
    except:
        app.logger.error("File not found for model {0}".format(modelname))
        return("File not found for model {0}".format(modelname), 404)

    final_result = df.loc[df["unique_identifier"] == unique_id]

    try:
        data = final_result.to_dict(orient='records')
        data = normalize(data)
    except:
        app.logger.error("Error reading file for model {0}".format(modelname))
        return("Error reading file for model {0}".format(modelname), 400)
    return jsonify(data)

app.run(port=5000)