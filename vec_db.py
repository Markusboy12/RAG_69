from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
import os

class QdrantStorage:
    def __init__(
        self,
        url=None,
        collection="docs",
        dim=384,
        api_key=None
    ):
        # ИСПРАВЛЕНО: Убраны пробелы в ключах и значениях
        self.url = url or os.getenv("QDRANT_URL", "http://localhost:6333")
        self.api_key = api_key or os.getenv("QDRANT_API_KEY")

        self.client = QdrantClient(
            url=self.url,
            api_key=self.api_key,
            timeout=30
        )
        self.collection = collection

        # ИСПРАВЛЕНО: Правильное имя метода collection_exists (без пробелов)
        if not self.client.collection_exists(self.collection):
            self.client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )

    def upsert(self, ids, vectors, payloads):
        points = [PointStruct(id=ids[i], vector=vectors[i], payload=payloads[i]) for i in range(len(ids))]
        # ИСПРАВЛЕНО: Правильное имя метода upsert (без пробелов)
        self.client.upsert(self.collection, points=points)

    def search(self, query_vector, top_k: int = 5):
        # ИСПРАВЛЕНО: Правильное имя метода query_points и аргумента query (без пробелов)
        results = self.client.query_points(
            collection_name=self.collection,
            query=query_vector,
            with_payload=True,
            limit=top_k
        )

        contexts = []
        sources = set()

        for r in results.points:
            # ИСПРАВЛЕНО: Убраны пробелы в ключах словаря
            payload = getattr(r, "payload", None) or {}
            text = payload.get("text", "")
            source = payload.get("source", "")
            if text:
                contexts.append(text)
                sources.add(source)

        return {"contexts": contexts, "sources": list(sources)}