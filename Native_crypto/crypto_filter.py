"""
Encryption Filter.

"""
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info


class EncryptionFilter(object):

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
                _, acc, cont, obj = req.split_path(4, 4, rest_with_last=True)
            except:
                # No object Request
                return req.get_response(self.app)

            self.logger.info('Encryption Filter - Going to encrypt '+req.path)
            req.environ['swift.crypto.encrypt'] = True

        return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def encryption_filter(app):
        return EncryptionFilter(app, conf)
    return encryption_filter
