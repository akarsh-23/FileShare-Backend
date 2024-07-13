import datetime
import io
import json
import logging
import uuid
import zipfile
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobType, BlobSasPermissions, generate_blob_sas
import os
from pymongo import MongoClient

app = func.FunctionApp()

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

# Get container name
container_name = os.environ["ContainerName"]

# Initialize MongoDB client
mongo_connection_string = os.environ["MongoDBConnectionString"]
mongo_db_name = "fileshare"  # Replace with your MongoDB database name
mongo_collection_name = "files"  # Replace with your MongoDB collection name
mongo_client = MongoClient(mongo_connection_string)
database = mongo_client[mongo_db_name]
collection = database[mongo_collection_name]

@app.route(route="upload/{user}/{folder}", auth_level=func.AuthLevel.ANONYMOUS)
def upload(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get route params
        user = req.route_params.get("user")
        folder = req.route_params.get("folder")

        if not user or not folder:
            logging.warning(f"User: {user}, Folder: {folder}")
            raise ValueError("User and folder parameters are required.")
        else:
            logging.info(f"User: {user}, Folder: {folder}")
        
        # Check if container exists, create if not
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()

        # Process each file in the request
        for file_key in req.files.keys():
            file = req.files[file_key]
            file_extension = os.path.splitext(file.filename)[1]
            file_name = str(uuid.uuid4()) + file_extension
            blob_name = f"{user}/{folder}/{file_name}"
            file_content = file.stream.read()

            # Upload file to Azure Blob Storage
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(file_content, blob_type=BlobType.BlockBlob)

        response_message = {
            "message": f"File(s) has been uploaded successfully."
        }
        return func.HttpResponse(json.dumps(response_message), status_code=200, mimetype="application/json")
    
    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
        error_message = {
            "error": f"ValueError: {ve}"
        }
        return func.HttpResponse(json.dumps(error_message), status_code=400, mimetype="application/json")
    
    except KeyError as ke:
        logging.error(f"KeyError: {ke}")
        error_message = {
            "error": f"KeyError: {ke}"
        }
        return func.HttpResponse(json.dumps(error_message), status_code=400, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        error_message = {
            "error": "Failed to upload files."
        }
        return func.HttpResponse(json.dumps(error_message), status_code=500, mimetype="application/json")

@app.blob_trigger(arg_name="blob", path="fileshare/{user}/{folder}/{filename}", connection="AzureWebJobsStorage") 
def upload_data(blob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob Name: {blob.name} Blob Size: {blob.length} bytes")
    
    try:
        blob = blob.name
        # Assuming the path is "fileshare/{user}/{folder}/{filename}"
        parts = blob.split('/')
        if len(parts) != 4:
            raise ValueError("Blob name does not match expected format 'fileshare/{user}/{folder}/{filename}'")

        container, user, folder, filename = parts
        
        # Generate SAS URL for the blob
        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=container,
            blob_name=f"{user}/{folder}/{filename}",
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1)  # SAS token expiry time
        )
        sas_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{blob}?{sas_token}"

        # Construct document to insert into MongoDB
        file_document = {
            "user": user,
            "folder": folder,
            "filename": filename,
            "sas_url": sas_url,
            "blob_name": f"{user}/{folder}/{filename}",
            "timestamp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }

        # Insert document into MongoDB
        result = collection.insert_one(file_document)
        logging.info(f"Inserted document with id: {result.inserted_id}")

    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
    except KeyError as ke:
        logging.error(f"KeyError: {ke}")
    except Exception as e:
        logging.error(f"Error processing blob {blob.name}: {e}")
        raise e
    
@app.route(route="files/{user}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_files(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user = req.route_params.get('user')
        response = collection.find({"user":user})
        documents = list(response)

        if len(documents)>0:
            return func.HttpResponse(json.dumps(documents, default=str), status_code=200, mimetype="application/json")
        else:
            error_message = {
                "error": f"Files not found for user: {user}"
            }
            return func.HttpResponse(json.dumps(error_message), status_code=404, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        error_message = {
            "error": "Error retrieving files."
        }
        return func.HttpResponse(json.dumps(error_message), status_code=500, mimetype="application/json")
    
@app.route(route="files/download-selected", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def download_files(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse the list of selected image filenames from the request body
        req_body = req.get_json()
        image_filenames = req_body.get("selected_images", [])
        
        if not image_filenames:
            return func.HttpResponse("No image filenames provided.", status_code=400)
        
        # Create a BytesIO buffer to hold the ZIP file in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for filename in image_filenames:
                blob_name = f"{filename}"
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                blob_data = blob_client.download_blob().readall()
                zip_file.writestr(filename, blob_data)
        
        # Seek to the beginning of the BytesIO buffer to read the data
        zip_buffer.seek(0)
        
        # Create HTTP response with ZIP file
        response = func.HttpResponse(zip_buffer.read(), status_code=200)
        response.headers["Content-Disposition"] = "attachment; filename=selected_images.zip"
        response.headers["Content-Type"] = "application/zip"
        
        return response

    except Exception as e:
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)