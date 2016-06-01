from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import get_logger
import sds_bandwidth_control as bc
import redis


class NotSDSBandwidthRequest(Exception):
    pass


def _request_instance_property():
    """
    Set and retrieve the request instance.
    This works to force to tie the consistency between the request path and
    self.vars (i.e. api_version, account, container, obj) even if unexpectedly
    (separately) assigned.
    """

    def getter(self):
        return self._request

    def setter(self, request):
        self._request = request
        try:
            self._extract_vaco()
        except ValueError:
            raise NotSDSBandwidthRequest()
        
        
    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class BaseSDSBandwidthHandler(object):
    """
    This is an abstract handler for Proxy/Object Server middleware
    """
    request = _request_instance_property()

    def __init__(self, request, conf, app, logger):
        """
        :param request: swob.Request instance
        :param conf: gatway conf dict
        """
        self.request = request
        self.server = conf.get('execution_server')

        self.app = app
        self.logger = logger
        self.conf = conf
        
        self.redis_host = conf.get('redis_host')
        self.redis_port = conf.get('redis_port')
        self.redis_db = conf.get('redis_db')
        
        self.method = self.request.method.lower()
        
        self.redis_connection = redis.StrictRedis(self.redis_host, 
                                                  self.redis_port, 
                                                  self.redis_db)
        

        ''' Singleton instance of bandwidth control '''
        if self.is_bandwidth_differentiation_server():
            self.bwc = bc.BandwidthControl.Instance(conf=self.conf,
                                                    logger=logger)
        
    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        if self.is_ssync_request():
            self._api_version = self._account = self._container = self._obj = 0
        else:
            self._api_version, self._account, self._container, self._obj = \
                self._parse_vaco()

    @property
    def api_version(self):
        return self._api_version

    @property
    def account(self):
        return self._account

    @property
    def container(self):
        return self._container

    @property
    def obj(self):
        return self._obj

    def _parse_vaco(self):
        """
        Parse method of path from self.request which depends on child class
        (Proxy or Object)
        :return tuple: a string tuple of (version, account, container, object)
        """
        raise NotImplementedError()

    def handle_request(self):
        
        if self.is_bandwidth_differentiation_server() and self.is_bw_diff_activated():
            if hasattr(self, self.request.method):
                resp = getattr(self, self.request.method)()
                return resp
            else:
                return self.request.get_response(self.app)
        else:
            self.logger.info('SDS Bandwidth differentiation - Disabled'
                             ' for this server')
            return self.request.get_response(self.app)


    def is_bw_diff_activated(self):
        #return self.redis_connection.get("bw_differentiation") == 'True'
        return True

    def is_ssync_request(self):
        return self._request.method == "SSYNC"
    
    def is_bandwidth_differentiation_server(self):
        return self.conf.get('bandwidth_control') == self.server

    def apply_bandwidth_differentiation_on_get(self, original_response): 
        self.bwc.register_response(self.account, original_response)

    def apply_bandwidth_differentiation_on_put(self):
        self.bwc.register_request(self.account,self.request)
    
    def apply_bandwidth_differentiation_on_ssync(self):
        self.bwc.register_ssync(self.request)


class SDSBandwidthProxyHandler(BaseSDSBandwidthHandler):

    def __init__(self, request, conf, app, logger):        
        super(SDSBandwidthProxyHandler, self).__init__(
              request, conf, app, logger)

    def _parse_vaco(self):
        return self.request.split_path(4, 4, rest_with_last=True)
        
    def GET(self):
        """
        GET handler on Proxy
        """    
        original_response = self.request.get_response(self.app)
        self.apply_bandwidth_differentiation_on_get(original_response) 
        
        return original_response

    def PUT(self):
        """
        PUT handler on Proxy
        """
        self.apply_bandwidth_differentiation_on_put()

        return self.request.get_response(self.app)


class SDSBandwidthObjectHandler(BaseSDSBandwidthHandler):

    def __init__(self, request, conf, app, logger):
        super(SDSBandwidthObjectHandler, self).__init__(
            request, conf, app, logger) 
        
        self.device = self.request.environ['PATH_INFO'].split('/',2)[1]

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.request.split_path(
            5, 5, rest_with_last=True)
        return ('0', acc, cont, obj)
    
    def _get_device(self, response):
        return response.environ['PATH_INFO'].split('/',2)[1]

    def GET(self):
        """
        GET handler on Object Server
        """    
        response = self.request.get_response(self.app)
        response.headers['device'] = self._get_device(response)
        self.apply_bandwidth_differentiation_on_get(response) 

        return response
                
    def PUT(self):
        """
        PUT handler on Object Server
        """
        self.apply_bandwidth_differentiation_on_put()

        return self.request.get_response(self.app)
    
    def SSYNC(self):
        """
        SSYNC handler on Object Server
        """       
        self.apply_bandwidth_differentiation_on_ssync()
        
        return self.request.get_response(self.app)
        

class SDSBandwidthHandlerMiddleware(object):

    def __init__(self, app, conf, sds_conf):
        self.app = app
        self.exec_server = sds_conf.get('execution_server')
        self.logger = get_logger(conf, log_route='sds_bandwidth_handler')
        self.sds_conf = sds_conf
 
        self.handler_class = self._get_handler(self.exec_server)
        
    def _get_handler(self, exec_server):
        if exec_server == 'proxy':
            return SDSBandwidthProxyHandler
        elif exec_server == 'object':
            return SDSBandwidthObjectHandler
        else:
            raise ValueError('configuration error: execution_server must'
                ' be either proxy or object but is %s' % exec_server)

    @wsgify
    def __call__(self, req):
        try:
            request_handler = self.handler_class(
                req, self.sds_conf, self.app, self.logger)
            self.logger.debug('sds_bandwidth_handler call in %s: with %s/%s/%s' %
                              (self.exec_server, request_handler.account,
                               request_handler.container,
                               request_handler.obj))
        except HTTPException:
            raise
        except NotSDSBandwidthRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('SDS bandwidth differentiation'
                                  ' execution failed')
            raise
        except Exception:
            self.logger.exception('SDS bandwidth differentiation'
                                  ' execution failed')
            raise HTTPInternalServerError(body='SDS bandwidth differentiation'
                                          ' execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    
    conf = global_conf.copy()
    conf.update(local_conf)
    
    sds_conf = dict()
    sds_conf['execution_server'] = conf.get('execution_server', 'object')
    
    sds_conf['redis_host'] = conf.get('redis_host', 'controller')
    sds_conf['redis_port'] = conf.get('redis_port', 6379)
    sds_conf['redis_db'] = conf.get('redis_db', 0)
    
    sds_conf['rabbit_host'] = conf.get('rabbit_host', 'controller')
    sds_conf['rabbit_port'] = conf.get('rabbit_port', 5672)
    sds_conf['rabbit_username'] = conf.get('rabbit_username', 'openstack')
    sds_conf['rabbit_password'] = conf.get('rabbit_password', 
                                           'rabbitmqastl1a4b4')
    
    sds_conf['consumer_tag'] = conf.get('consumer_tag', 'bw_assignations')
    sds_conf['routing_key_get'] = conf.get('routing_key_get', 
                                           'bwdifferentiation.get_bw_info')
    sds_conf['routing_key_put'] = conf.get('routing_key_put', 
                                           'bwdifferentiation.put_bw_info')
    sds_conf['routing_key_ssync'] = conf.get('routing_key_ssync', 
                                             'bwdifferentiation.ssync_bw_info')
    sds_conf['exchange_osinfo'] = conf.get('exchange_osinfo', 'amq.topic')
    sds_conf['interval_osinfo'] = conf.get('interval_osinfo', 0.2)
    sds_conf['bandwidth_control'] = conf.get('bandwidth_control', 'proxy')
    
    sds_conf['replication_one_per_dev'] = conf.get('replication_one_per_dev',
                                                   False)
    
    sds_conf['bind_ip'] = conf.get('bind_ip')
    sds_conf['bind_port'] = conf.get('bind_port')
        
    def sds_bandwidth_differentiation(app):
        return SDSBandwidthHandlerMiddleware(app, conf, sds_conf)

    return sds_bandwidth_differentiation
