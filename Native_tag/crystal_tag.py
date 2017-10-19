"""
MetadataEnhancer Filter.
This filter adds specific tags based on file name patterns.
"""
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info
import re


class MetadataEnhancer(object):

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
        if req.method == 'PUT':
            regex = None
            tag = None

            if 'regex' in self.parameters:
                regex = self.parameters['regex']
            if 'tag' in self.parameters:
                tag = self.parameters['tag']

            if regex and tag:
                _, _, _, obj = req.split_path(4, 4, rest_with_last=True)
                pattern = re.compile(regex)
                if pattern.search(obj):
                    header, value = tag.split(':')
                    self.logger.info('Tagging Filter - Adding '+tag+' tag to the object')
                    req.headers['X-Object-Meta-'+header] = value
            else:
                self.logger.error('Tagging Filter - No regex or tag provided')
            return req.get_response(self.app)
        else:
            return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def metadata_enhancer_filter(app):
        return MetadataEnhancer(app, conf)
    return metadata_enhancer_filter
