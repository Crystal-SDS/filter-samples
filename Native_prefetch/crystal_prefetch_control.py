import hashlib
import time
import os
import select
import swiftclient

from crystal_filter_middleware.filters.abstract_filter import AbstractFilter
from eventlet import Timeout
from swift.common.internal_client import InternalClient
from swift.common.swob import Request, Response
from threading import Semaphore, Lock, Thread


ENABLE_CACHE = True
# Cache size limit in bytes
CACHE_MAX_SIZE = 20*1024*1024
available_policies = {"LRU", "LFU"}
CACHE_PATH = "/tmp/cache/prefetch"
#CACHE_PATH = "/media/ramdisk"
#CACHE_PATH = "/mnt/ssd/"
CHUNK_SIZE = 65536
PROXY_PATH = '/etc/swift/proxy-server.conf'

ACCEPTABLE_STATUS = [200]
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'


class Singleton(type):
    _instances = {}

    # To have a thread-safe Singleton
    __lock = Lock()

    def __call__(cls, *args, **kwargs):  # @NoSelf
        if cls not in cls._instances:
            with cls.__lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


# crystal_prefetch_control.PrefetchControl has 3 points of interception:
# 1. pre-get: to check if the file is in the cache
# 2. post-get: to fill prefetching cache (with internal requests)
class PrefetchControl(AbstractFilter):

    __metaclass__ = Singleton

    def __init__(self, global_conf, filter_conf, logger):
        super(PrefetchControl, self).__init__(global_conf, filter_conf, logger)
        self.cache = BlockCache()
        self.logger = logger

        # TODO Fill cache with prefetched files
        self.logger.info("In PrefetchControl.__init__")
        self.prefetch_thread = Thread(target=self.init_prefetching)
        self.prefetch_thread.daemon = True
        self.prefetch_thread.start()

    def _get_object(self, req_resp, crystal_iter):

        if isinstance(req_resp, Request):
            # millis_start = int(round(time.time() * 1000))
            # self.logger.info('SDS Cache Filter - Timestamp: ' + str(millis_start))

            if 'X-Crystal-Prefetch' in req_resp.headers:
                # Let pass this request to the object server
                req_resp.headers.pop('X-Crystal-Prefetch', None)
                return req_resp.environ['wsgi.input']
            else:
                # Check if Object is in cache
                if os.path.exists(CACHE_PATH):

                    object_path = req_resp.environ['PATH_INFO']
                    object_id = (hashlib.md5(object_path).hexdigest())

                    object_id, object_size, object_etag = self.cache.access_cache("GET", object_id)

                    if object_id:
                        self.logger.info('Prefetch Filter - Object '+object_path+' in cache')
                        resp_headers = {}
                        resp_headers['Content-Length'] = str(object_size)
                        resp_headers['Etag'] = object_etag

                        cached_object_fd = os.open(CACHE_PATH + object_id, os.O_RDONLY)

                        # TODO: Return headers if necessary
                        req_resp.response_headers = resp_headers
                        return FpIter(cached_object_fd, 10)

        # elif isinstance(req_resp, Response):
        #
        #     if os.path.exists(CACHE_PATH):
        #         object_path = req_resp.environ['PATH_INFO']
        #         object_size = int(req_resp.headers.get('Content-Length', ''))
        #         object_etag = req_resp.headers.get('ETag', '')
        #         object_id = (hashlib.md5(object_path).hexdigest())
        #
        #         to_evict = self.cache.access_cache("PUT", object_id, object_size, object_etag)
        #         for object_id in to_evict:
        #             os.remove(CACHE_PATH + object_id)
        #
        #         self.logger.info('Prefetch Filter (POST-GET) - Object ' + object_path + ' stored in cache with ID: ' + object_id)
        #
        #         cached_object = open(CACHE_PATH + object_id, 'w')
        #         return DataIter(crystal_iter, 10, cached_object, self._filter_put)

        return req_resp.environ['wsgi.input']

    def _filter_put(self, cached_object, chunk):
        cached_object.write(chunk)
        return chunk

    def init_prefetching(self):
        #while self.alive:
        self.logger.info('In init_prefetching')
        time.sleep(2)

        tenant_id = '366756dbfd024e0aa7f204a7498dfcfa'
        container = 'data1'
        object_name = 'template.tex'
        object_path = '/v1/AUTH_' + tenant_id + '/' + container + '/' + object_name
        object_id = hashlib.md5(object_path).hexdigest()

        if not self.token:
            self._get_admin_token()

        self.download(object_id, tenant_id, container, object_name, USER_AGENT, self.token)

    def download(self, oid, acc, container, name, u_agent, token, delay=0, request_tries=3):
        print 'Prefetching object with InternalClient: ' + oid + ' after ' + str(delay.total_seconds()) + ' seconds of delay.'
        time.sleep(delay.total_seconds())
        swift = InternalClient(PROXY_PATH, u_agent, request_tries=request_tries)
        headers = {}
        headers['X-Auth-Token'] = token
        headers['X-No-Prefetch'] = 'True'
        status, head, it = swift.get_object(acc, container, name, headers, ACCEPTABLE_STATUS)
        data = [el for el in it]
        self.logger.info(str(data))
        # return oid, data, head

    def _get_admin_token(self):
        """
        Method called to obtain the admin credentials, which we need to made requests to Swift
        """
        admin_project = 'service'
        admin_user = 'admin'
        admin_passwd = 'admin'
        admin_keystone_url = 'http://localhost:35357/v3'
        try:
            _, self.token = swiftclient.client.get_auth(admin_keystone_url,
                                                        admin_project + ":" + admin_user,
                                                        admin_passwd, auth_version="3")
        except:
            self.logger.error("There was an error getting a token from keystone")
            raise Exception()


class CacheObjectDescriptor(object):

    def __init__(self, block_id, size, etag):
        self.block_id = block_id
        self.last_access = time.time()
        self.get_hits = 0
        self.put_hits = 0
        self.num_accesses = 0
        self.size = size
        self.etag = etag

    def get_hit(self):
        self.get_hits += 1
        self.hit()

    def put_hit(self):
        self.put_hits += 1
        self.hit()

    def hit(self):
        self.last_access = time.time()
        self.num_accesses += 1


class BlockCache(object):

    def __init__(self):
        # This will contain the actual data of each block
        self.descriptors_dict = {}
        # Structure to store the cache metadata of each block
        self.descriptors = []
        # Cache statistics
        self.get_hits = 0
        self.put_hits = 0
        self.misses = 0
        self.evictions = 0
        self.reads = 0
        self.writes = 0
        self.cache_size_bytes = 0

        # Eviction policy
        self.policy = "LFU"
        # Synchronize shared cache content
        self.semaphore = Semaphore()

    def access_cache(self, operation='PUT', block_id=None, block_data=None, etag=None):
        result = None
        if ENABLE_CACHE:
            self.semaphore.acquire()
            if operation == 'PUT':
                result = self._put(block_id, block_data, etag)
            elif operation == 'GET':
                result = self._get(block_id)
            else:
                raise Exception("Unsupported cache operation" + operation)
            # Sort descriptors based on eviction policy order
            self._sort_descriptors()
            self.semaphore.release()
        return result

    def _put(self, block_id, block_size, etag):
        self.writes += 1
        to_evict = []
        # Check if the cache is full and if the element is new
        if CACHE_MAX_SIZE <= (self.cache_size_bytes + block_size) and block_id not in self.descriptors_dict:
            # Evict as many files as necessary until having enough space for new one
            while (CACHE_MAX_SIZE <= (self.cache_size_bytes + block_size)):
                # Get the last element ordered by the eviction policy
                self.descriptors, evicted = self.descriptors[:-1], self.descriptors[-1]
                # Reduce the size of the cache
                self.cache_size_bytes -= evicted.size
                # Increase evictions count and add to
                self.evictions += 1
                to_evict.append(evicted.block_id)
                # Remove from evictions dict
                del self.descriptors_dict[evicted.block_id]

        if block_id in self.descriptors_dict:
            descriptor = self.descriptors_dict[block_id]
            self.descriptors_dict[block_id].size = block_size
            self.descriptors_dict[block_id].etag = etag
            descriptor.put_hit()
            self.put_hits += 1
        else:
            # Add the new element to the cache
            descriptor = CacheObjectDescriptor(block_id, block_size, etag)
            self.descriptors.append(descriptor)
            self.descriptors_dict[block_id] = descriptor
            self.cache_size_bytes += block_size

        assert len(self.descriptors) == len(self.descriptors_dict.keys()) ==\
            len(self.descriptors_dict.keys()), "Unequal length in cache data structures"

        return to_evict

    def _get(self, block_id):
        self.reads += 1
        if block_id in self.descriptors_dict:
            self.descriptors_dict[block_id].get_hit()
            self.get_hits += 1
            return block_id, self.descriptors_dict[block_id].size, self.descriptors_dict[block_id].etag
        self.misses += 1
        return None, 0, ''

    def _sort_descriptors(self):
        # Order the descriptor list depending on the policy
        if self.policy == "LRU":
            self.descriptors.sort(key=lambda desc: desc.last_access, reverse=True)
        elif self.policy == "LFU":
            self.descriptors.sort(key=lambda desc: desc.get_hits, reverse=True)
        else:
            raise Exception("Unsupported caching policy.")

    def write_statistics(self):
        if ENABLE_CACHE:
            self.cache_state()

    def cache_state(self):
        print "CACHE GET HITS: ", self.get_hits
        print "CACHE PUT HITS: ", self.put_hits
        print "CACHE MISSES: ", self.misses
        print "CACHE EVICTIONS: ", self.evictions
        print "CACHE READS: ", self.reads
        print "CACHE WRITES: ", self.writes
        print "CACHE SIZE: ", self.cache_size_bytes

        for descriptor in self.descriptors:
            print "Object: ", descriptor.block_id, descriptor.last_access, descriptor.get_hits, descriptor.put_hits, descriptor.num_accesses, descriptor.size


class DataIter(object):
    def __init__(self, obj_data, timeout, cached_object, filter_method):
        self.closed = False
        self.obj_data = obj_data
        self.timeout = timeout
        self.buf = b''
        self.cached_object = cached_object

        self.filter = filter_method

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                if hasattr(self.obj_data, 'read'):
                    chunk = self.obj_data.read(size)
                else:
                    chunk = self.obj_data.next()
                chunk = self.filter(self.cached_object, chunk)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise

        return chunk

    def next(self, size=CHUNK_SIZE):
        if len(self.buf) < size:
            self.buf += self.read_with_timeout(size - len(self.buf))
            if self.buf == b'':
                self.close()
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def _close_check(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def read(self, size=64 * 1024):
        self._close_check()
        return self.next(size)

    def readline(self, size=-1):
        self._close_check()

        # read data into self.buf if there is not enough data
        while b'\n' not in self.buf and \
              (size < 0 or len(self.buf) < size):
            if size < 0:
                chunk = self.read()
            else:
                chunk = self.read(size - len(self.buf))
            if not chunk:
                break
            self.buf += chunk

        # Retrieve one line from buf
        data, sep, rest = self.buf.partition(b'\n')
        data += sep
        self.buf = rest

        # cut out size from retrieved line
        if size >= 0 and len(data) > size:
            self.buf = data[size:] + self.buf
            data = data[:size]

        return data

    def readlines(self, sizehint=-1):
        self._close_check()
        lines = []
        try:
            while True:
                line = self.readline(sizehint)
                if not line:
                    break
                lines.append(line)
                if sizehint >= 0:
                    sizehint -= len(line)
                    if sizehint <= 0:
                        break
        except StopIteration:
            pass
        return lines

    def close(self):
        if self.closed:
            return
        try:
            self.cached_object.close()
            self.obj_data.close()
        except AttributeError:
            pass
        self.closed = True

    def __del__(self):
        self.close()


class FpIter(object):
    def __init__(self, obj_data, timeout):
        self.closed = False
        self.obj_data = obj_data
        self.timeout = timeout
        self.buf = b''

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = os.read(self.obj_data, size)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise
        return chunk

    def next(self, size=64 * 1024):
        if len(self.buf) < size:
            r, _, _ = select.select([self.obj_data], [], [], self.timeout)
            if len(r) == 0:
                self.close()

            if self.obj_data in r:
                self.buf += self.read_with_timeout(size - len(self.buf))
                if self.buf == b'':
                    self.close()
                    raise StopIteration('Stopped iterator ex')
            else:
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def _close_check(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def read(self, size=CHUNK_SIZE):
        self._close_check()
        return self.next(size)

    def readline(self, size=-1):
        self._close_check()

        # read data into self.buf if there is not enough data
        while b'\n' not in self.buf and \
              (size < 0 or len(self.buf) < size):
            if size < 0:
                chunk = self.read()
            else:
                chunk = self.read(size - len(self.buf))
            if not chunk:
                break
            self.buf += chunk

        # Retrieve one line from buf
        data, sep, rest = self.buf.partition(b'\n')
        data += sep
        self.buf = rest

        # cut out size from retrieved line
        if size >= 0 and len(data) > size:
            self.buf = data[size:] + self.buf
            data = data[:size]

        return data

    def readlines(self, sizehint=-1):
        self._close_check()
        lines = []
        try:
            while True:
                line = self.readline(sizehint)
                if not line:
                    break
                lines.append(line)
                if sizehint >= 0:
                    sizehint -= len(line)
                    if sizehint <= 0:
                        break
        except StopIteration:
            pass
        return lines

    def close(self):
        if self.closed:
            return
        os.close(self.obj_data)
        self.closed = True

    def __del__(self):
        self.close()
