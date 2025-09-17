import os
import werkzeug
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
from chromadb import PersistentClient
client = PersistentClient(path="chroma_db_storage")
from sentence_transformers import SentenceTransformer


model = SentenceTransformer("all-MiniLM-L6-v2")

collection = client.get_or_create_collection(name="argo_float_profiles")


NC_STORAGE_PATH = "data"

app = Flask(__name__)
api = Api(app)
CORS(app)

# --- Ensure storage directory exists ---
os.makedirs(NC_STORAGE_PATH, exist_ok=True)


# In REST, a "Resource" is the core concept. Here, our resource is a NetCDF file.
# This class will handle all requests related to the collection of files.
class NetCDFFileList(Resource):
    def post(self):
        """
        Handles creating new file resources. Corresponds to the POST HTTP method.
        This endpoint is designed to receive a batch of files.
        """
        # Set up a parser to find the files in the request
        parser = reqparse.RequestParser()
        parser.add_argument(
            'files',
            type=werkzeug.datastructures.FileStorage,
            location='files',
            action='append' # Use 'append' to handle multiple files with the same key
        )
        args = parser.parse_args()
        
        uploaded_files = args.get('files')
        if not uploaded_files:
            return {'message': 'No files found in the request.'}, 400

        results = []
        for file in uploaded_files:
            filename = werkzeug.utils.secure_filename(file.filename)

            # Validate file type
            if not filename.endswith('.nc'):
                results.append({
                    "filename": filename,
                    "status": "error",
                    "detail": "Invalid file type. Only .nc files are accepted."
                })
                continue

            #Save the file
            file_path = os.path.join(NC_STORAGE_PATH, filename)
            try:
                file.save(file_path)
                results.append({
                    "filename": filename,
                    "status": "success",
                    "stored_path": file_path
                })
            except Exception as e:
                results.append({
                    "filename": filename,
                    "status": "error",
                    "detail": f"An error occurred during save: {str(e)}"
                })

        # Return the result
        # A 201 "Created" status is often used for a successful POST request.
        return {"upload_summary": results}, 201


# --- Map the Resource to a URL Endpoint ---
# We are mapping our resource class to the '/files' endpoint.
# POST requests to /files will be handled by the post() method in NetCDFFileList.
api.add_resource(NetCDFFileList, '/files')

@app.route('/get-question', methods=['POST'])
def query():
    data = request.json
    user_query = data.get("prompt")

    query_embedding = model.encode([user_query]).tolist()

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=3
    )

    return jsonify({"response":f"{results['documents'][0][0]} \n\n {results['documents'][0][1]}"})


if __name__ == '__main__':
    app.run(debug=True, port=5000, host = "0.0.0.0")