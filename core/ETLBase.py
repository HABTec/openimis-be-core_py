import os
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from opensearchpy import NotFoundError, OpenSearch



class ETLBase(ABC):
    def __init__(self) -> None:
        self.BATCH_SIZE = int(os.environ.get("ETL_BATCH_SIZE", 1000))
        self.DATABASE = os.environ.get("POSTGRES_DB", "dashboard_db")

        self.engine: Engine = self._create_postgres_engine()

        self.opensearch = self._create_opensearch_client()

    def _create_postgres_engine(self) -> Engine:
        user = os.environ.get("PSQL_DB_USER")
        password = os.environ.get("PSQL_DB_PASSWORD")
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("PSQL_DB_PORT", "5432")
        db = os.environ.get("PSQL_DB_NAME")

        if not all([user, password, db]):
            raise RuntimeError("PostgreSQL environment variables are missing")

        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

        return create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

    def _create_opensearch_client(self) -> OpenSearch:
        host = os.environ.get("OPENSEARCH_HOST", "localhost")
        port = int(os.environ.get("OPENSEARCH_PORT", 9200))
        user = os.environ.get("OPENSEARCH_ADMIN")
        password = os.environ.get("OPENSEARCH_PASSWORD")
        use_ssl = os.environ.get("OPENSEARCH_USE_SSL", "false").lower() == "true"

        return OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(user, password) if user else None,
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )

    @abstractmethod
    def process(self):
        """Main ETL entry point"""
        pass

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        """Load data into OpenSearch"""
        pass
    def clear_index(self, index_name: str):
        """
        Delete all documents from the given OpenSearch index
        """
        try:
            if self.opensearch.indices.exists(index=index_name):
                self.opensearch.delete_by_query(
                    index=index_name,
                    body={
                        "query": {"match_all": {}}
                    },
                    refresh=True,
                    conflicts="proceed"
                )

        except NotFoundError:
            print(f"[ETL] Index not found: {index_name}")