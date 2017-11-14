"""
Tagging Filter.
This filter adds specific tags based on file name patterns.
"""
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info
import re


class TaggingFilter(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='tagging_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']

        self.register_info()

    def register_info(self):
        register_swift_info('tagging_filter')

    @wsgify
    def __call__(self, req):
        if req.method == 'PUT':
            try:
                # This filter is applied only to the objects
                _, _, _, obj = req.split_path(4, 4, rest_with_last=True)
            except:
                return req.get_response(self.app)

            regex = None
            tag = None

            if 'regex' in self.parameters:
                regex = self.parameters['regex']
            if 'tag' in self.parameters:
                tag = self.parameters['tag']

            if regex and tag:
                pattern = re.compile(regex)
                if pattern.search(obj):
                    key, value = tag.split(':')
                    self.logger.info('Tagging Filter - Adding '+tag+' tag to the object')
                    req.headers['X-Object-Sysmeta-'+key] = value
            elif tag:
                self.logger.info('Tagging Filter - Adding '+tag+' tag to the object')
                key, value = tag.split(':')
                req.headers['X-Object-Sysmeta-'+key] = value
            else:
                self.logger.error('Tagging Filter - No regex or tag provided')
            return req.get_response(self.app)
        else:
            return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def tagging_filter(app):
        return TaggingFilter(app, conf)
    return tagging_filter
