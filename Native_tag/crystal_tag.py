from crystal_filter_middleware.filters.abstract_filter import AbstractFilter
import re

"""
MetadataEnhancer Filter.
This filter adds specific tags based on file name patterns.
"""


class MetadataEnhancer(AbstractFilter):

    def __init__(self, global_conf, filter_conf, logger):
        super(MetadataEnhancer, self).__init__(global_conf, filter_conf, logger)

    def _apply_filter(self, req_resp, data_iter, parameters):
        method = req_resp.environ['REQUEST_METHOD']

        if method == 'PUT':
            return self._put_object(req_resp, data_iter, parameters)
        else:
            return data_iter

    def _put_object(self, request, data_iter, parameters):
        regex = None
        tag = None

        if 'regex' in parameters:
            regex = parameters['regex']
        if 'tag' in parameters:
            tag = parameters['tag']

        if regex and tag:
            _, _, _, obj = request.split_path(4, 4, rest_with_last=True)
            pattern = re.compile(regex)
            if pattern.search(obj):
                header, value = tag.split(':')
                self.logger.info('Tagging Filter - Adding '+value+' tag to the object')
                request.headers['X-Object-Meta-'+header] = value
        else:
            self.logger.error('Tagging Filter - No regex or tag provided')

        # This filter does not modifies the original data, so we return
        # the original data_iter.
        return request.environ['wsgi.input']
