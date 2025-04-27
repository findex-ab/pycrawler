from qdrant_client import QdrantClient, models

def setup_qdrant(qdrant: QdrantClient) -> QdrantClient:
    if not qdrant.collection_exists('crawler_website'):
        qdrant.create_collection(
            'crawler_website',
            vectors_config=models.VectorParams(size=100, distance=models.Distance.COSINE)
        )
    return qdrant


def qdrant_connect(connection_string: str = "http://localhost:6333"):
    qdrant = QdrantClient(connection_string) # Connect to existing Qdrant instance
    return setup_qdrant(qdrant)

