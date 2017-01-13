from crystal_filter_middleware.filters.abstract_filter import AbstractFilter, IterLike


class NoopFilter(AbstractFilter):

    def __init__(self, global_conf, filter_conf, logger):
        super(AbstractFilter, self).__init__(global_conf, filter_conf, logger)
