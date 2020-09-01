

class DatasetBaseException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)


class InternalDatasetException(DatasetBaseException):
    """
    Another dataset run is not finalized.
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)


class AlreadyActiveException(DatasetBaseException):
    """
    Another dataset run is not finalized.
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)

class RunAlreadyFinishedException(DatasetBaseException):
    """
    Another dataset run is already finalized.
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)


class DatasetNotFoundException(DatasetBaseException):
    """
    dataset not found
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)

class RunNotFoundException(DatasetBaseException):
    """
    dataset not found
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)


class DependencyException(DatasetBaseException):
    """
    Dependency(s) not found.
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)

class ConfigValidationException(DatasetBaseException):
    """
    Validation or configuration problem.
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)

class APIValidationException(DatasetBaseException):
    """
    Validation or configuration problem.
    """
    def __init__(self, *args, **kwargs):
        DatasetBaseException.__init__(self, *args, **kwargs)

