"""
Recycle Bin Filter.
This filter copies an object to a recycle bin for a time when it is deleted.
To execute in Proxy Servers.
"""
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info
from swift.common.wsgi import make_subrequest
import os

DEFAULT_MAX_TIME = 86400 * 30


class RecycleBinFilter(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='recyclebin_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']

        self.register_info()

    def register_info(self):
        register_swift_info('recyclebin_filter')

    def _parse_vaco(self, req):
        self.api, self.account, self.container, self.obj = req.split_path(4, 4, rest_with_last=True)

    @wsgify
    def __call__(self, req):
        if req.method == 'DELETE':
            self._parse_vaco(req)
            if self.obj and not self.container.startswith('.bin_'):
                new_env = req.environ.copy()
                recyclebin_container = '.bin_'+self.container
                auth_token = req.headers.get('X-Auth-Token')
                new_container = os.path.join('/', self.api, self.account, recyclebin_container)
                new_path = os.path.join(recyclebin_container, self.obj)

                sub_req = make_subrequest(new_env, 'PUT', new_container,
                                          headers={'X-Auth-Token': auth_token},
                                          swift_source='recyclebin_filter')
                resp = sub_req.get_response(self.app)

                if 'max_time' in self.parameters:
                    max_time = self.parameters['max_time']
                else:
                    max_time = DEFAULT_MAX_TIME

                sub_req = make_subrequest(new_env, 'COPY', req.path,
                                          headers={'X-Auth-Token': auth_token,
                                                   'X-Delete-After': max_time,
                                                   'Destination': new_path},
                                          swift_source='recyclebin_filter')
                resp = sub_req.get_response(self.app)

                if resp.is_success:
                    return req.get_response(self.app)
                else:
                    return resp

        return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def recyclebin_filter(app):
        return RecycleBinFilter(app, conf)
    return recyclebin_filter
