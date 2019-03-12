import logging
from contextlib import contextmanager
from datetime import datetime
import pymongo
from pymongo import MongoClient


@contextmanager
def Mongo(**kwargs):
    client = MongoClient(**kwargs)
    try:
        yield client
    finally:
        client.close()


class SimpleMongoLog(logging.Handler):
    """Simple logging handler to send logging message to a Mongo database.

        For each individual message, a new connection is established,
        the message is inserted and the connection is closed.
           - Be aware that connection establishment is a blocking operation,
                which means that the execution incurs in a significant overhead.
                Such overhead is unacceptable in high frequency logging.
           - Closing the connection after message insertion frees database
                connection congestion.

        This handler is recommended for low frequency logging.

        Note:
            Some attention should be paid to some exceptions that might
            occur during connection, create and insert operations. Check
            http://api.mongodb.com/python/current/api/pymongo/errors.html

        Note:
            This handler inserts a new document for each logging message into
            the collection. The use of a capped collection is highly recommended
            for better performance.
            Check https://docs.mongodb.com/manual/core/capped-collections/
    """

    def __init__(self, level=logging.DEBUG, database='log', collection='log',
                 create_collection=False, collection_kwargs={}, **kwargs):
        """Initialise handler.

            Args:
                level (int): Minimum reporting log level.
                database (str): Database name.
                collection (str): Collection name.
                create_collection (bool): If True it tries to create a new collection
                    with the arguments specified in :obj:`collection_kwargs`.
                collection_args (dict): Dictionary with the arguments to pass to
                :obj:`create_collection` method. Check
                    http://api.mongodb.com/python/current/api/pymongo/database.html#pymongo.database.Database.create_collection

            kwargs are passed directly to :obj:`MongoClient` constructor. Check
                http://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
        """

        logging.Handler.__init__(self, level)
        self.database = database
        self.collection = collection
        self.kwargs = kwargs

        if create_collection:
            self._create_collection(collection_kwargs, **kwargs)

    def _create_collection(self, collection_kwargs, **kwargs):
        """Creates a new collection with the arguments specified.

            If a collection with the same name already exists,
            then no action is performed on the database.
        """

        try:
            with Mongo(**kwargs) as mongo:
                db = mongo[self.database]
                if self.collection not in db.list_collection_names():
                    db.create_collection(name=self.collection, **collection_kwargs)
        except pymongo.errors.ConnectionFailure as cf:
            # Handle the exception here
            pass

    def emit(self, record):
        """Insert the record into the Mongo collection"""

        try:
            with Mongo(**self.kwargs) as mongo:
                mongo[self.database][self.collection].insert_one({
                    'datetime': datetime.utcfromtimestamp(record.created),
                    'processName': record.processName,
                    'processId': record.process,
                    'threadName': record.threadName,
                    'threadId': record.thread,
                    'pathname': record.pathname,
                    'filename': record.filename,
                    'module': record.module,
                    'funcName': record.funcName,
                    'lineno': record.lineno,
                    'msg': record.msg,
                    'levelname': record.levelname,
                    'levelno': record.levelno,
                    # 'funcargs': record.args,
                })
        except pymongo.errors.WriteError as we:
            # Handle the exception here
            pass
        except pymongo.errors.ConnectionFailure as cf:
            # Handle the exception here
            pass
