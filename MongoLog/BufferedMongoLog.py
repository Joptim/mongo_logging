import logging
from logging.handlers import MemoryHandler
from contextlib import contextmanager
from datetime import datetime
from pymongo import MongoClient


@contextmanager
def Mongo(**kwargs):

    client = MongoClient(**kwargs)
    try:
        yield client
    finally:
        client.close()


class BufferedMongoLog(MemoryHandler):
    """Buffered logging handler to send logging messages to a Mongo database.

        Note:
            The buffer is flushed when it gets to its full capacity or when an important
            log message is emitted. Flushing implies establishing a new connection
            to a Mongo database, inserting all buffered logs, closing the connection
            and deleting the inserted messages.

            This handler is suitable for low-medium frequency logging.

        Note:
            This handler inserts a new document for each logging message into the collection.
            The use of a capped collection is highly recommended for better performance.
            Check https://docs.mongodb.com/manual/core/capped-collections/
    """

    def __init__(self, capacity, flushLevel=logging.ERROR,
                 database='log', collection='log',
                 create_collection=False, collection_kwargs={}, **kwargs):

        """Initialise handler.

            Args:
                capacity: Maximum number of log messages buffered.
                flushLevel: The buffer will be flushed when a message with a level
                    equal or higher to :obj:`flushLevel` is emitted.
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

        MemoryHandler.__init__(self, capacity, flushLevel=flushLevel)

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

        with Mongo(**kwargs) as mongo:
            db = mongo[self.database]
            if self.collection not in db.list_collection_names():
                db.create_collection(name=self.collection, **collection_kwargs)

    def flush(self):
        """Insert all buffered records into the mongo collection"""

        import time
        time.sleep(1)

        self.acquire()
        try:
            if not self.buffer:
                return

            buffer_len = len(self.buffer)

            with Mongo(**self.kwargs) as mongo:
                mongo[self.database][self.collection].insert_many([{
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
                } for record in self.buffer[:buffer_len]])

            self.buffer[:buffer_len] = []
        finally:
            self.release()
