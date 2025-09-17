from chromadb import PersistentClient  # for disk persistence
client = PersistentClient(path="chroma_db_storage")
from sentence_transformers import SentenceTransformer


model = SentenceTransformer("all-MiniLM-L6-v2")

collection = client.get_or_create_collection(name="argo_float_profiles")

# print(collection.peek())
# print(collection.get())



def get_chunks(query):
    query_embedding = model.encode([query]).tolist()

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=3
    )

    # results = collection.encode(query).tolist()

    print("🔎 Query Results:")
    print(results['documents'][0][0], "\n\n", results['documents'][0][1], "\n\n", results['documents'][0][2])

get_chunks('what are the approximate temperature and salinity values near the surface and at about 2000 dbar depth, and how do they change with depth?')