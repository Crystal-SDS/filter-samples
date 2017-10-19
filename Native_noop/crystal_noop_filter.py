"""
No-operation Filter.
This filter does nothing.
"""
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info


class NoopFilter(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='metadata_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']

        self.register_info()

    def register_info(self):
        register_swift_info('metadata_filter')

    @wsgify
    def __call__(self, req):
        return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def noop_filter(app):
        return NoopFilter(app, conf)
    return noop_filter
