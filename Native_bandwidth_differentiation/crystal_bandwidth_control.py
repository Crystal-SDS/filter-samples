from crystal_filter_middleware.filters.abstract_filter import AbstractFilter
from threading import Thread
from eventlet import Timeout
from swift.common.request_helpers import SegmentedIterable
import eventlet
import select
import Queue
import time
import pika
import os
import redis
import threading
import socket


CHUNK_SIZE = 65536
MB = 1024*1024.
# Check and control the data flow every interval
SLEEP_CALCULATION_INTERVAL = 0.2
# Maximum throughput of a single node in the system (Gb Ethernet = 110MB (APPROX))
BW_MAX = 115


class Singleton(type):
    _instances = {}
    __lock = threading.Lock()

    def __call__(cls, *args, **kwargs):  # @NoSelf
        if cls not in cls._instances:
            with cls.__lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BandwidthControl(AbstractFilter):
    __metaclass__ = Singleton

    def __init__(self, global_conf, filter_conf, logger):
        super(BandwidthControl, self).__init__(global_conf, filter_conf, logger)
        self.server = self.global_conf.get('execution_server')
        redis_host = self.global_conf['redis_host']
        redis_port = int(self.global_conf['redis_port'])
        redis_db = self.global_conf['redis_db']

        self.redis = redis.StrictRedis(redis_host, redis_port, redis_db)

        self.get_bw_control = {}
        self.put_bw_control = {}

        self._start_useless_threads_monitoring()
        self._start_assignments_consumer()

    def _apply_filter(self, req_resp, data_iter, parameters):
        """
        Entry Method
        """
        method = req_resp.environ['REQUEST_METHOD']

        if method == 'GET':
            return self._get_object(req_resp, data_iter)

        elif method == 'PUT':
            return self._put_object(req_resp, data_iter)

    def _get_project_id(self, req_resp):
        """
        This method returns the current project ID
        """
        return req_resp.environ['PATH_INFO'].split('/')[2].split('_')[1]

    def _get_storage_policy_id(self, req_resp):
        """
        This method returns the current storage policy ID
        """
        if self.server == "proxy":
            project = req_resp.environ['PATH_INFO'].split('/')[2]
            container = req_resp.environ['PATH_INFO'].split('/')[3]
            info = req_resp.environ['swift.infocache']
            storage_policy = info['container/'+project+'/'+container]['storage_policy']
        else:
            storage_policy = req_resp.environ['HTTP_X_BACKEND_STORAGE_POLICY_INDEX']

        return storage_policy

    def _put_object(self, request, data_iter):
        r, w = os.pipe()
        write_pipe = os.fdopen(w, 'w')

        read_pipe = data_iter

        project = self._get_project_id(request)
        storage_policy = self._get_storage_policy_id(request)

        thd_id = project+"-"+storage_policy

        if thd_id not in self.put_bw_control:
            self.logger.info("Bandwidth Differentiation - Creating new "
                             "PUT thread: "+project+":"+storage_policy)

            redis_bw = self.redis.get('SLO:bandwidth:put_bw:'+project+'#'+storage_policy)
            if redis_bw is not None:
                initial_bw = int(redis_bw)
            else:
                initial_bw = BW_MAX

            thr = BandwidthThreadControl(thd_id, self.logger, initial_bw)
            self.put_bw_control[thd_id] = thr
            thr.daemon = True
            thr.start()
        else:
            thr = self.put_bw_control[thd_id]

        self.logger.info("Bandwidth Differentiation - Add stream to tenant")
        thr.add_stream(write_pipe, read_pipe)

        return DataFdIter(r, 10)

    def _get_object(self, response, data_iter):
        r, w = os.pipe()
        write_pipe = os.fdopen(w, 'w')

        if self.server == 'object':
            read_pipe = data_iter._fp
        else:
            read_pipe = data_iter
            if isinstance(read_pipe, SegmentedIterable):
                read_pipe = read_pipe.app_iter

        project = self._get_project_id(response)
        storage_policy = self._get_storage_policy_id(response)

        thd_id = project+"-"+storage_policy
        if thd_id not in self.get_bw_control:
            self.logger.info("Bandwidth Differentiation - Creating new "
                             "GET thread: "+project+":"+storage_policy)

            redis_bw = self.redis.get('SLO:bandwidth:get_bw:'+project+'#'+storage_policy)
            if redis_bw is not None:
                initial_bw = int(redis_bw)
            else:
                initial_bw = BW_MAX

            thr = BandwidthThreadControl(thd_id, self.logger, initial_bw)
            self.get_bw_control[thd_id] = thr
            thr.daemon = True
            thr.start()
        else:
            thr = self.get_bw_control[thd_id]

        thr.add_stream(write_pipe, read_pipe)

        return DataFdIter(r, 10)

    def _start_useless_threads_monitoring(self):
        """
        This method starts the threads responsible of cleaning threads dictionaries.
        """
        self.logger.info("Bandwidth Differentiation - Starting useless threads "
                         "monitoring")
        thbw_get = Thread(target=self._kill_threads, args=(self.get_bw_control,))
        thbw_get.start()

        thbw_put = Thread(target=self._kill_threads, args=(self.put_bw_control,))
        thbw_put.start()

    def _kill_threads(self, threads):
        """
        This methdod checks the threads which are already stopped,
        and deletes them from the main dictionary.
        """
        while True:
            eventlet.sleep(2)
            '''Clean useless threads'''
            for thread_key in threads.keys():
                if not threads[thread_key].alive:
                    self.logger.info("Bandwidth Differentiation - Killing "
                                     "thread "+thread_key)
                    del threads[thread_key]

    def _start_assignments_consumer(self):
        self.logger.info("Bandwidth Differentiation - Starting bandwidth "
                         "assignments consumer")

        rabbit_host = 'controller'
        rabbit_port = 5672
        rabbit_user = 'openstack'
        rabbit_pass = 'openstack'
        exchange = 'amq.topic'

        host_name = socket.gethostname()
        routing_key = host_name
        queue_bw = "bandwidth_assignations:"+host_name

        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        parameters = pika.ConnectionParameters(host=rabbit_host,
                                               port=rabbit_port,
                                               credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_bw)
        channel.queue_bind(exchange=exchange, routing_key=routing_key, queue=queue_bw)
        channel.basic_consume(self._bw_assignations, queue=queue_bw, no_ack=True)

        thbw_assignation = Thread(target=channel.start_consuming)
        thbw_assignation.start()

    def _bw_assignations(self, ch, m, properties, body):
        project, method, storage_policy, bw = body.split('/')
        thread_key = project+":"+storage_policy
        if method == "get":
            try:
                self.get_bw_control[thread_key].update_bw_limit(float(bw))
            except KeyError:
                pass
        elif method == "put":
            try:
                self.put_bw_control[thread_key].update_bw_limit(float(bw))
            except KeyError:
                pass


class BandwidthThreadControl(Thread):

    def __init__(self, thd_id, log, initial_bw):
        Thread.__init__(self)
        self.thd_id = thd_id
        self.logger = log

        # Last instant at which a bw calculation has been done
        self.streams = Queue.Queue()

        # Timeout for reading stream from the streams queue
        self.timeout = 10

        # This sleep value changes depending on the bw rate configured
        self.dynamic_sleep = 0.0

        # Bytes transferred in the present bw control interval
        self.transferred_bytes_control = 0

        # Number of data flow iterations within control interval
        self.number_of_iterations = 0

        # Bandwidth limit
        self.bandwidth_limit = initial_bw

        # To stop useless threads
        self.alive = True

        # Calculate initial sleep
        self._calculate_initial_sleep()

        # Control periodically the enforcement of bw limits
        self.rate_control = Thread(target=self._rate_control)
        self.rate_control.daemon = True
        self.rate_control.start()

    def add_stream(self, write_pipe, read_pipe):
        pipe_tuple = (write_pipe, read_pipe)
        self.streams.put(pipe_tuple)

    def update_bw_limit(self, limit):
        if limit == 0.0:
            limit = 1.0

        self.bandwidth_limit = limit
        self._calculate_initial_sleep()

    def _calculate_initial_sleep(self):
        """
        This method calculates the initial sleep based on some magic numbers.
         ** EXPERIMENTAL **
        """
        BEST_CHUNK_READ_TIME = 0.0002

        self.max_iterations = int(round(((self.bandwidth_limit * SLEEP_CALCULATION_INTERVAL) * MB) / CHUNK_SIZE))
        read_chunk = BEST_CHUNK_READ_TIME * self.max_iterations
        self.calc_sleep = ((SLEEP_CALCULATION_INTERVAL - read_chunk) / self.max_iterations)

        self.dynamic_sleep_mean = self.calc_sleep
        self.dynamic_sleep = self.calc_sleep
        self.total_sleep_mean = self.calc_sleep
        self.iters_mean = 1
        self.counter = 0
        self.bad_counter = 0

    def _rate_control(self):
        while self.alive:
            time.sleep(SLEEP_CALCULATION_INTERVAL)
            # Copy required counters
            trabsferred_bytes = self.transferred_bytes_control
            iterations = self.number_of_iterations

            # Reset counters for new interval calculation
            self.transferred_bytes_control = 0
            self.number_of_iterations = 0

            # Estimate the current transfer bw
            mb = float(trabsferred_bytes/MB)
            bandwidth_estimation = round(mb/SLEEP_CALCULATION_INTERVAL, 2)

            # Calculate the deviation in percentage
            slo_deviation = abs(bandwidth_estimation - self.bandwidth_limit)
            slo_deviation_percentage = (slo_deviation*100)/self.bandwidth_limit

            # Apply the calculated percentage in the current dynamic_sleep
            sleep_deviation = (self.dynamic_sleep*slo_deviation_percentage)/100

            # Add or substract the sleep_deviation to calculate the new dynamic_sleep
            if bandwidth_estimation < self.bandwidth_limit:
                new_dynamic_sleep = self.dynamic_sleep - sleep_deviation
            elif bandwidth_estimation > self.bandwidth_limit:
                new_dynamic_sleep = self.dynamic_sleep + sleep_deviation

            previous_dynamic_sleep = self.dynamic_sleep

            if slo_deviation < 1.5:
                self.iters_mean += 1
                self.total_sleep_mean += new_dynamic_sleep
                self.dynamic_sleep_mean = self.total_sleep_mean/self.iters_mean

            if new_dynamic_sleep/self.dynamic_sleep_mean < 1.2 and new_dynamic_sleep/self.dynamic_sleep_mean > 0.7:
                self.counter += 1
                self.bad_counter = 0
                self.dynamic_sleep = abs(new_dynamic_sleep)
            else:
                self.counter = 0
                self.bad_counter += 1

                if self.bad_counter > 10:
                    self.dynamic_sleep = new_dynamic_sleep
                    self.bad_counter = 0
                else:
                    self.dynamic_sleep = self.dynamic_sleep_mean

            f = open("/tmp/bw_control_"+self.thd_id+".dat", 'a+')
            f.write("\n##############################\n")
            f.write("Required Bandwidth : "+str(self.bandwidth_limit)+"\n")
            f.write("Current Bandwidth  : "+str(bandwidth_estimation)+"\n")
            f.write("-----\n")
            f.write("Required Iterations : "+str(self.max_iterations)+"\n")
            f.write("Current Iterations  : "+str(iterations)+"\n")
            f.write("-----\n")
            f.write("Prev. dynamic sleep: "+'{0:.6f}'.format(round(previous_dynamic_sleep, 6))+"\n")
            f.write("New dynamic sleep  : "+'{0:.6f}'.format(round(new_dynamic_sleep, 6))+"\n")
            f.write("-----\n")
            f.write("Mean sleep  : "+'{0:.6f}'.format(round(self.dynamic_sleep_mean, 6))+"\n")
            f.write("Calc. sleep : "+'{0:.6f}'.format(round(self.calc_sleep, 6))+"\n")
            f.write("-----\n")
            f.write("In Range sleep     : "+str(self.counter)+"\n")
            f.write("Not in range sleep : "+str(self.bad_counter)+"\n")
            f.close()

    def _read_chunk(self, reader):
        try:
            if hasattr(reader, 'read'):
                chunk = reader.read(CHUNK_SIZE)
            else:
                chunk = reader.next()
            return chunk
        except StopIteration:
            return False

    def _write_with_timeout(self, writer, chunk):
        try:
            with Timeout(self.timeout):
                writer.write(chunk)
        except Timeout:
            writer.close()
            raise

    def run(self):
        while self.alive:
            try:
                stream_data = self.streams.get(timeout=self.timeout)

                (writer, reader) = stream_data
                request_finished = False
                '''
                To share bandwidth_limit among several requests,
                provide to each request a proportional number
                of chunk transfer operations
                '''
                proportional_bw = 16

                for _ in range(proportional_bw):
                    try:
                        chunk = self._read_chunk(reader)
                        if chunk:
                            self._write_with_timeout(writer, chunk)
                            # Account for the transferred data
                            self.transferred_bytes_control += len(chunk)
                            self.number_of_iterations += 1
                            # Wait to achieve a certain bw rate on this flow
                            time.sleep(max(0, self.dynamic_sleep))
                        else:
                            if hasattr(reader, 'close'):
                                reader.close()
                            writer.close()
                            request_finished = True
                            break

                    except IOError as e:
                        request_finished = True
                        self.logger.info("Pipe error: " + str(e))
                        break
                    except Exception as e:
                        request_finished = True
                        self.logger.info("An unknown error occurred during "
                                         "transfer: " + str(e))
                        break
                    except Timeout as e:
                        request_finished = True
                        self.logger.info("Timeout occurred during transfer: " + str(e))
                        break

                if not request_finished:
                    self.streams.put(stream_data)

            except Queue.Empty:
                self.alive = False


class DataFdIter(object):
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
        os.close(self.obj_data)
        self.closed = True

    def __del__(self):
        self.close()
