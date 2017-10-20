"""
Metadata blocker Filter.
This filter blocks the possibility to update specific metadata tags by external users.
parameter: tags=slash/separated/tags
"""
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info


class MetadataBlockerFilter(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='metadatablocker_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']

        self.register_info()

    def register_info(self):
        register_swift_info('metadatablocker_filter')

    @wsgify
    def __call__(self, req):
        if req.method in ('PUT', 'POST'):
            blocked_tags = self.parameters['tags'].split('/')
            print blocked_tags, req.headers
            for tag in blocked_tags:
                if 'X-Object-Meta-'+tag in req.headers:
                    self.logger.info('Metadata Blocker Filter - "'+tag+'" tag not allowed')
                    del req.headers['X-Object-Meta-'+tag]

        return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def noop_filter(app):
        return MetadataBlockerFilter(app, conf)
    return noop_filter
