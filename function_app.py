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
from pymongo.errors import ConnectionFailure, PyMongoError

import os, pymongo
import logging
from llama_index.core import Settings, VectorStoreIndex, get_response_synthesizer
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.azurecosmosmongo import AzureCosmosDBMongoDBVectorSearch
from urllib.parse import quote_plus
from llama_index.core.retrievers import VectorIndexRetriever
from llama_index.core.query_engine import RetrieverQueryEngine
from llama_index.core.postprocessor import SimilarityPostprocessor

from llama_index.core import Document, StorageContext, Settings
from llama_index.core.ingestion import IngestionPipeline, DocstoreStrategy
from llama_index.storage.docstore.mongodb import MongoDocumentStore

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

@app.route(route="roles", auth_level=func.AuthLevel.ANONYMOUS)
def roles(req: func.HttpRequest) -> func.HttpResponse:
    # return func.HttpResponse(json.dumps({"roles":["admin"]}), status_code=200, mimetype="application/json")
    logging.info('Python HTTP trigger function processed a request.')
    try:
        # Parse the request body
        data = json.loads(req.get_body().decode('utf-8'))
        logging.info("data: {data}".format(data=json.dumps(data)))

        # Extract userId
        user_id = data.get("userId")
        if not user_id:
            logging.error("Missing 'userId' in 'clientPrincipal'")
            raise ValueError("Missing 'userId' in 'clientPrincipal'")

        # Check if the user exists in the database
        collection = database["users"]
        response = collection.find_one({"user": user_id})
        logging.info("Response: {response}".format(response=response))
        if response:
            roles = response.get("roles", [])
            logging.info(f"User found. Attached roles: {roles}")
            return func.HttpResponse(json.dumps({"roles": roles}), status_code=200, mimetype="application/json")

        # Extract other details from clientPrincipal
        identity_provider = data.get("identityProvider")
        claims = data.get("claims", [])
        claims_dict = {claim.get("typ"): claim.get("val") for claim in claims}

        email = claims_dict.get("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress")
        name = claims_dict.get("name")
        picture = claims_dict.get("picture")

        # Prepare the document to insert into the database
        document = {
            "name": name,
            "user": user_id,
            "email": email,
            "picture": picture,
            "identity_provider": identity_provider,
        }

        # Insert the document into the database
        result = collection.insert_one(document)
        logging.info(f"Inserted document with id: {result.inserted_id}")

        roles = document.get("roles", [])
        return func.HttpResponse(json.dumps({"roles": roles}), status_code=200, mimetype="application/json")

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from request body")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON format"}),
            status_code=400,
            mimetype="application/json"
        )
    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
        return func.HttpResponse(
            json.dumps({"error": str(ve)}),
            status_code=400,
            mimetype="application/json"
        )
    except ConnectionFailure:
        logging.error("Failed to connect to MongoDB")
        return func.HttpResponse(
            json.dumps({"error": "Failed to connect to database"}),
            status_code=500,
            mimetype="application/json"
        )
    except PyMongoError as e:
        logging.error(f"MongoDB error: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Database error occurred"}),
            status_code=500,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return func.HttpResponse(
            json.dumps({"error": "An unexpected error occurred"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="user/{user_id}", auth_level=func.AuthLevel.ANONYMOUS)
def get_user(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        # Extract userId
        user_id = req.route_params.get("user_id")
        logging.info(json.dumps(dict(req.headers), indent=4))
        if not user_id:
            logging.error("Missing 'userId'")
            raise ValueError("Missing 'userId'")

        # Check if the user exists in the database
        collection = database["users"]
        response = collection.find_one({"user": user_id})
        logging.info("Response: {response}".format(response=response))
        if response:
            logging.info(f"User found. User: {response}")
            return func.HttpResponse(json.dumps(response, default=str), status_code=200, mimetype="application/json")
        return func.HttpResponse(json.dumps({"error": "User not found"}), status_code=404, mimetype="application/json")

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON format"}),
            status_code=400,
            mimetype="application/json"
        )
    except ValueError as ve:
        logging.error(f"ValueError: {ve}")
        return func.HttpResponse(
            json.dumps({"error": str(ve)}),
            status_code=400,
            mimetype="application/json"
        )
    except ConnectionFailure:
        logging.error("Failed to connect to MongoDB")
        return func.HttpResponse(
            json.dumps({"error": "Failed to connect to database"}),
            status_code=500,
            mimetype="application/json"
        )
    except PyMongoError as e:
        logging.error(f"MongoDB error: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Database error occurred"}),
            status_code=500,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return func.HttpResponse(
            json.dumps({"error": "An unexpected error occurred"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="response", auth_level=func.AuthLevel.ANONYMOUS)
def response(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # MongoDB connection setup
    username = quote_plus("akarsh23")
    password = quote_plus("Saksham@2301")
    uri = f"mongodb+srv://{username}:{password}@ai-chat.mongocluster.cosmos.azure.com?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
    logging.info("MongoDB connection URI constructed")

    # Initialize LLM and embedding model
    Settings.llm = OpenAI()
    Settings.embed_model = OpenAIEmbedding()
    logging.info("OpenAI LLM and embedding model initialized")

    # Setup Vector Store
    logging.debug("Connecting to MongoDB at %s", uri)
    client = pymongo.MongoClient(str(uri))
    vector_store = AzureCosmosDBMongoDBVectorSearch(
        client,
        db_name="vector_store",
        collection_name="data",
        id_key="_id",
        embedding_key="embedding",
        metadata_key="metadata",
        cosmos_search_kwargs={"kind":"vector-ivf"}
    )
    logging.info("Vector store initialized successfully")

    # Create index from vector store
    index = VectorStoreIndex.from_vector_store(
        vector_store=vector_store,
        embed_model=Settings.embed_model,
        show_proress=True
    )
    logging.info("Vector store index created")

    # Setup retriever and query engine
    retriever = VectorIndexRetriever(index=index, similarity_top_k=100)
    response_synthesizer = get_response_synthesizer(llm=Settings.llm, streaming=True)
    query_engine = RetrieverQueryEngine(
        retriever=retriever,
        response_synthesizer=response_synthesizer,
        node_postprocessors=[SimilarityPostprocessor(similarity_cutoff=0.7)],
    )
    logging.info("Retriever and query engine initialized")

    query = req.params.get("query", "how can you help me?")
    logging.info("Received request with query: %s", query)

    try:
        # Execute query
        logging.info("Executing query: %s", query)
        result = query_engine.query(query)
        logging.info("Query executed successfully")

    except Exception as e:
        logging.error("An error occurred during request processing: %s", str(e))
        raise e

    response = {
        "data": result
    }
    logging.info("Response: {response}".format(response=response))
    if response:
        return func.HttpResponse(json.dumps(response, default=str), status_code=200, mimetype="application/json")
    else:
        return func.HttpResponse(
             f"This {result} triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )