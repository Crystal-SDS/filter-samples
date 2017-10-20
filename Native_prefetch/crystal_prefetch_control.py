import hashlib
import time
import os
import select

from crystal_filter_middleware.filters.abstract_filter import AbstractFilter
from eventlet import Timeout
from swift.common.internal_client import InternalClient
from swift.common.swob import Request, Response
from threading import Semaphore, Lock, Thread


ENABLE_CACHE = True
# Default cache size limit in bytes
DEFAULT_CACHE_MAX_SIZE = 1024 * 1024  # 1 MB
available_policies = {"LRU", "LFU"}
DEFAULT_CACHE_PATH = "/tmp/cache"
DEFAULT_EVICTION_POLICY = "LFU"
CHUNK_SIZE = 65536
PROXY_PATH = '/etc/swift/local-proxy-server-prefetch.conf'
ACCEPTABLE_STATUS = [200]
USER_AGENT = 'Crystal Filter Internal Client'


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


class PrefetchControl(AbstractFilter):
    """
    crystal_prefetch_control.PrefetchControl has 1 point of interception:
    1. pre-get: to check if the file is in the cache
    The prefetch cache is filled on initialization (according to params cache_max_size, tenant_id and container.
    """

    __metaclass__ = Singleton

    def __init__(self, global_conf, filter_conf, logger):
        super(PrefetchControl, self).__init__(global_conf, filter_conf, logger)
        self.logger = logger

        self.cache_max_size = self._get_cache_max_size()
        self.cache_path = self._get_cache_path()
        self.eviction_policy = self._get_eviction_policy()
        self.tenant_id, self.container = self._get_tenant_container()

        self.cache = BlockCache(self.cache_max_size, self.eviction_policy)

        # Fill cache with prefetched files
        self.prefetch_thread = Thread(target=self.init_prefetching)
        self.prefetch_thread.daemon = True
        self.prefetch_thread.start()

    def _get_cache_max_size(self):
        cache_max_size = DEFAULT_CACHE_MAX_SIZE
        if self.filter_conf['params']:
            if 'cache_max_size' in self.filter_conf['params']:
                cache_max_size = int(self.filter_conf['params']['cache_max_size'])
        return cache_max_size

    def _get_cache_path(self):
        cache_path = DEFAULT_CACHE_PATH
        if self.filter_conf['params']:
            if 'cache_path' in self.filter_conf['params']:
                cache_path = self.filter_conf['params']['cache_path']
        return cache_path

    def _get_eviction_policy(self):
        eviction_policy = DEFAULT_EVICTION_POLICY
        if self.filter_conf['params']:
            if 'eviction_policy' in self.filter_conf['params']:
                eviction_policy = self.filter_conf['params']['eviction_policy']
        return eviction_policy

    def _get_tenant_container(self):
        tenant_id = 'dummy'
        container = 'dummy'
        if self.filter_conf['params']:
            if 'tenant_id' in self.filter_conf['params']:
                tenant_id = self.filter_conf['params']['tenant_id']
            if 'container' in self.filter_conf['params']:
                container = self.filter_conf['params']['container']
        return tenant_id, container

    def _get_object(self, req_resp, crystal_iter):

        if isinstance(req_resp, Request):

            # Check if Object is in cache
            if os.path.exists(self.cache_path):

                object_path = req_resp.environ['PATH_INFO']
                object_id = (hashlib.md5(object_path).hexdigest())

                object_id, object_size, object_etag, object_storage_policy_id = self.cache.access_cache("GET", object_id)

                if object_id:
                    self.logger.info('Prefetch Filter - Object ' + object_path + ' found in cache')
                    resp_headers = {}
                    resp_headers['Content-Length'] = str(object_size)
                    resp_headers['Etag'] = object_etag

                    cached_object_fd = os.open(os.path.join(self.cache_path, object_id), os.O_RDONLY)

                    req_resp.response_headers = resp_headers
                    req_resp.environ['HTTP_X_BACKEND_STORAGE_POLICY_INDEX'] = object_storage_policy_id
                    return FdIter(cached_object_fd, 10)

        return req_resp.environ['wsgi.input']

    def _filter_put(self, cached_object, chunk):
        cached_object.write(chunk)
        return chunk

    def init_prefetching(self):
        time.sleep(1)

        account = 'AUTH_' + self.tenant_id
        self.download(account, self.container, USER_AGENT)

    def download(self, acc, container, u_agent, delay=0, request_tries=3):
        self.logger.info('Prefetching objects with InternalClient with ' + str(delay) + ' seconds of delay.')
        time.sleep(delay)
        swift = InternalClient(PROXY_PATH, u_agent, request_tries=request_tries)
        headers = {}

        prefetch_list = []
        bytes_count = 0
        for o in swift.iter_objects(acc, container):
            if bytes_count + int(o['bytes']) < self.cache_max_size:
                prefetch_list.append(o['name'])
                bytes_count += int(o['bytes'])
            else:
                break

        for name in prefetch_list:
            object_path = '/v1/' + acc + '/' + container + '/' + name
            oid = hashlib.md5(object_path).hexdigest()

            status, resp_headers, it = swift.get_object(acc, container, name, headers, ACCEPTABLE_STATUS)

            object_size = int(resp_headers.get('Content-Length'))
            object_etag = resp_headers.get('Etag')

            object_storage_policy_id = '0'  # FIXME hardcoded
            to_evict = self.cache.access_cache("PUT", oid, object_size, object_etag, object_storage_policy_id)
            for ev_object_id in to_evict:
                os.remove(os.path.join(self.cache_path, ev_object_id))

            self.logger.info('Prefetch Filter - Object ' + name + ' stored in cache with ID: ' + oid)
            with open(os.path.join(self.cache_path, oid), 'w') as f:
                for el in it:
                    f.write(el)


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
            while self.cache_max_size <= (self.cache_size_bytes + block_size):
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
