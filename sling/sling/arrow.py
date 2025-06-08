# Create a fake pyarrow module for compilation
class FakeArrowType:
    def __init__(self, *args, **kwargs):
        pass
    
    def __call__(self, *args, **kwargs):
        return self
    
    def __getattr__(self, name):
        return FakeArrowType()

class FakeIPC:
    @staticmethod
    def new_stream(*args, **kwargs):
        return FakeArrowType()
    
    @staticmethod
    def open_stream(*args, **kwargs):
        return FakeArrowType()

class FakePA:
    Schema = FakeArrowType
    DataType = FakeArrowType
    RecordBatch = FakeArrowType
    ipc = FakeIPC()
    
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