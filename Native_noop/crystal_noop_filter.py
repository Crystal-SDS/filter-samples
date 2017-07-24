from crystal_filter_middleware.filters.abstract_filter import AbstractFilter


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):  # @NoSelf
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class NoopFilter(AbstractFilter):

    __metaclass__ = Singleton

    def __init__(self, global_conf, filter_conf, logger):
        super(NoopFilter, self).__init__(global_conf, filter_conf, logger)
