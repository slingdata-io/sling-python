# Create a fake pyarrow module for compilation
class FakeArrowType:
    def __init__(self, *args, **kwargs):
        pass
    
    def __call__(self, *args, **kwargs):
        return self
    
    def __getattr__(self, name):
        return FakeArrowType()

class FakeTable(FakeArrowType):
    @staticmethod
    def from_pandas(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod 
    def from_batches(*args, **kwargs):
        return FakeArrowType()

class FakeArrowInvalid(Exception):
    """Fake ArrowInvalid exception for when PyArrow is not available"""
    pass

class FakeLib:
    ArrowInvalid = FakeArrowInvalid

class FakeRecordBatchStreamReader:
    def __init__(self, *args, **kwargs):
        self._schema = FakeArrowType()
        self._closed = False
    
    def __iter__(self):
        return self
    
    def __next__(self):
        # For fake implementation, just raise StopIteration to end iteration
        raise StopIteration
    
    def read_next_batch(self):
        return FakeArrowType()
    
    def read_all(self):
        return FakeArrowType()
    
    @property
    def schema(self):
        return self._schema
    
    def close(self):
        self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

class FakeIPC:
    RecordBatchStreamReader = FakeRecordBatchStreamReader
    
    @staticmethod
    def new_stream(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def open_stream(*args, **kwargs):
        return FakeRecordBatchStreamReader()

class FakePA:
    Schema = FakeArrowType
    DataType = FakeArrowType
    RecordBatch = FakeArrowType
    Table = FakeTable
    ipc = FakeIPC()
    lib = FakeLib()
    
    @staticmethod
    def schema(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def field(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def string(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def bool_(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def int64(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def float64(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def timestamp(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def date32(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def array(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def record_batch(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def table(*args, **kwargs):
        return FakeArrowType()