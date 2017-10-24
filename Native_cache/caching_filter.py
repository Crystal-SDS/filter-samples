from eventlet import Timeout
from swift.common.swob import Response
from threading import Semaphore, Lock
from swift.common.utils import get_logger
from swift.common.swob import wsgify
from swift.common.utils import register_swift_info
import hashlib
import select
import time
import os

ENABLE_CACHE = True
# Default cache size limit in bytes
DEFAULT_CACHE_MAX_SIZE = 1024 * 1024  # 1 MB
available_policies = {"LRU", "LFU"}
DEFAULT_CACHE_PATH = "/tmp/cache"
DEFAULT_EVICTION_POLICY = "LFU"
CHUNK_SIZE = 65536


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


# crystal_cache_control.CacheControl has 3 points of interception:
# 1. pre-get: to check if the file is in the cache
# 2. post-get: to store an existing file in the cache
# 3. pre-put: to store a new file in the cache
class CacheControlFilter(object):

    __metaclass__ = Singleton

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='cache_control_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']
        self.register_info()

        self.cache_max_size = self._get_cache_max_size()
        self.cache_path = self._get_cache_path()
        self.eviction_policy = self._get_eviction_policy()

        self.cache = BlockCache(self.cache_max_size, self.eviction_policy)

    def register_info(self):
        register_swift_info('cache_control_filter')

    def _get_cache_max_size(self):
        cache_max_size = DEFAULT_CACHE_MAX_SIZE
        if 'cache_max_size' in self.parameters:
            cache_max_size = int(self.parameters['cache_max_size'])
        return cache_max_size

    def _get_cache_path(self):
        cache_path = DEFAULT_CACHE_PATH
        if 'cache_path' in self.parameters:
            cache_path = self.parameters['cache_path']
        return cache_path

    def _get_eviction_policy(self):
        eviction_policy = DEFAULT_EVICTION_POLICY
        if 'eviction_policy' in self.parameters:
            eviction_policy = self.parameters['eviction_policy']
        return eviction_policy

    @wsgify
    def __call__(self, req):
        object_path = req.environ['PATH_INFO']
        object_id = (hashlib.md5(object_path).hexdigest())
        if req.method == 'GET':
            if self.cache.is_object_in_cache(object_id):
                self.logger.info('Cache Filter - Object '+object_path+' in cache')
                return self.get_cahced_object(req, object_id)

            resp = req.get_response(self.app)
            if resp.is_success:
                data_iter = resp.app_iter
                resp.app_iter = self._put_object_in_cache(resp, object_id, data_iter)
            return resp

        elif req.method == 'PUT':
            reader = req.environ['wsgi.input'].read
            data_iter = iter(lambda: reader(65536), '')
            req.environ['wsgi.input'] = self._put_object_in_cache(req, object_id, data_iter)

        return req.get_response(self.app)

    def _get_cahced_object(self, req, object_id):
        object_id, object_size, object_etag, object_storage_policy_id = self.cache.access_cache("GET", object_id)

        if object_id:
            resp_headers = {}
            resp_headers['Content-Length'] = str(object_size)
            resp_headers['Etag'] = object_etag

            # Using os.open() instead of python open() to avoid sequentialization
            cached_object_fd = os.open(os.path.join(self.cache_path, object_id), os.O_RDONLY)

            req.environ['HTTP_X_BACKEND_STORAGE_POLICY_INDEX'] = object_storage_policy_id
            data_iter = FdIter(cached_object_fd, 10)

            return Response(app_iter=data_iter, headers=resp_headers, request=req)

    def _put_object_in_cache(self, req_resp, object_id, data_iter):
        if os.path.exists(self.cache_path):
            object_path = req_resp.environ['PATH_INFO']
            object_size = int(req_resp.headers.get('Content-Length', ''))
            object_etag = req_resp.headers.get('ETag', '')
            object_storage_policy_id = self._get_storage_policy_id(req_resp)
            object_id = (hashlib.md5(object_path).hexdigest())

            to_evict = self.cache.access_cache("PUT", object_id, object_size, object_etag, object_storage_policy_id)

            for ev_object_id in to_evict:
                os.remove(os.path.join(self.cache_path, ev_object_id))

            self.logger.info('Cache Filter - Storing object ' + object_path +
                             ' in cache with ID: ' + object_id)

            cached_object = open(os.path.join(self.cache_path, object_id), 'w')

            return DataIter(data_iter, 10, cached_object, self._write_object_into_cache)

    def _write_object_into_cache(self, cached_object, chunk):
        cached_object.write(chunk)
        return chunk

    def _get_storage_policy_id(self, request):
        if 'HTTP_X_BACKEND_STORAGE_POLICY_INDEX' in request.environ:
            storage_policy = request.environ['HTTP_X_BACKEND_STORAGE_POLICY_INDEX']
        else:
            project = request.environ['PATH_INFO'].split('/')[2]
            container = request.environ['PATH_INFO'].split('/')[3]
            info = request.environ['swift.infocache']
            storage_policy = info['container/' + project + '/' + container]['storage_policy']

        return storage_policy


class CacheObjectDescriptor(object):

    def __init__(self, block_id, size, etag, storage_policy_id):
        self.block_id = block_id
        self.last_access = time.time()
        self.get_hits = 0
        self.put_hits = 0
        self.num_accesses = 0
        self.size = size
        self.etag = etag
        self.storage_policy_id = storage_policy_id

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

    def __init__(self, cache_max_size, eviction_policy):
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
        self.cache_max_size = cache_max_size

        # Eviction policy
        self.policy = eviction_policy
        # Synchronize shared cache content
        self.semaphore = Semaphore()

    @property
    def is_objectin_cache(self, block_id):
        return block_id in self.descriptors_dict

    def access_cache(self, operation='PUT', block_id=None, block_data=None, etag=None, storage_policy_id=None):
        result = None
        if ENABLE_CACHE:
            self.semaphore.acquire()
            if operation == 'PUT':
                result = self._put(block_id, block_data, etag, storage_policy_id)
            elif operation == 'GET':
                result = self._get(block_id)
            else:
                raise Exception("Unsupported cache operation" + operation)
            # Sort descriptors based on eviction policy order
            self._sort_descriptors()
            self.semaphore.release()
        return result

    def _put(self, block_id, block_size, etag, storage_policy_id):
        self.writes += 1
        to_evict = []
        # Check if the cache is full and if the element is new
        if self.cache_max_size <= (self.cache_size_bytes + block_size) and block_id not in self.descriptors_dict:
            # Evict as many files as necessary until having enough space for new one
            while (self.cache_max_size <= (self.cache_size_bytes + block_size)):
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
            self.descriptors_dict[block_id].storage_policy_id = storage_policy_id
            descriptor.put_hit()
            self.put_hits += 1
        else:
            # Add the new element to the cache
            descriptor = CacheObjectDescriptor(block_id, block_size, etag, storage_policy_id)
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
            return block_id, self.descriptors_dict[block_id].size, self.descriptors_dict[block_id].etag, self.descriptors_dict[block_id].storage_policy_id
        self.misses += 1
        return None, 0, '', ''

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
        try:
            self.cached_object.close()
            self.obj_data.close()
        except AttributeError:
            pass
        self.closed = True

    def __del__(self):
        self.close()


class FdIter(DataIter):
    def __init__(self, obj_data, timeout):
        super(FdIter, self).__init__(obj_data, timeout, None, None)
        self.closed = False
        self.obj_data = obj_data
        self.timeout = timeout
        self.buf = b''

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

    def next(self, size=CHUNK_SIZE):
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

    def close(self):
        if self.closed:
            return
        os.close(self.obj_data)
        self.closed = True


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def cache_control_filter(app):
        return CacheControlFilter(app, conf)
    return cache_control_filter
