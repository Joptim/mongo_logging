import logging
from logging.handlers import MemoryHandler
from contextlib import contextmanager
from datetime import datetime
import pymongo
from pymongo import MongoClient, InsertOne


@contextmanager
def Mongo(**kwargs):
    client = MongoClient(**kwargs)
    try:
        yield client
    finally:
        client.close()


class BufferedMongoLog(MemoryHandler):
    """Buffered logging handler to send logging messages to a Mongo database.

        The buffer is flushed when it gets to its full capacity or when an important
        log message is emitted. Flushing implies establishing a new connection
        to a Mongo database, inserting all buffered logs, closing the connection
        and deleting the inserted messages.

        This handler is suitable for low-medium frequency logging.

        Note:
            Some attention should be paid to some exceptions that might
            occur during connection, create and insert operations. Check
            http://api.mongodb.com/python/current/api/pymongo/errors.html

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

        try:
            with Mongo(**kwargs) as mongo:
                db = mongo[self.database]
                if self.collection not in db.list_collection_names():
                    db.create_collection(name=self.collection, **collection_kwargs)
        except pymongo.errors.ConnectionFailure as cf:
            # Handle the exception here
            pass

    def flush(self):
        """Insert all buffered records into the Mongo collection.

            Note:
                Log records are inserted in chronological order into the database.
                The first insert failure that occurs aborts the remaining insert
                operations. All log records inserted successfully will be removed
                from the buffer.

        """

        self.acquire()

        try:
            if not self.buffer:
                return

            with Mongo(**self.kwargs) as mongo:
                bulk_result = mongo[self.database][self.collection].bulk_write([
                    InsertOne({
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
                    }) for record in self.buffer], ordered=True)

                if not bulk_result.acknowledged:
                    # Handle error here.
                    pass

            self.buffer[:bulk_result.inserted_count] = []

        except pymongo.errors.BulkWriteError as bwe:
            # Handle the exception here
            # Error details can be found in bwe._OperationFailure__details

            self.buffer[:bwe.details.get('nInserted', 0)] = []

        except pymongo.errors.ConnectionFailure as cf:
            # Handle the exception here
            pass

        finally:
            self.release()
