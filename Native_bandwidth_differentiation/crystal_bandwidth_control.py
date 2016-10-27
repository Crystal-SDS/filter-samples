from threading import Thread
from eventlet import Timeout
from swift.common.swob import Request
from swift.common.swob import Response
import eventlet
import select
import Queue
import time
import pika
import json
import copy
import os
import redis


CHUNK_SIZE = 65536
'''Check and control the data flow every interval'''
SAMPLE_CONTROL_INTERVAL = 0.1
MB = 1024 * 1024.
'''Maximum throughout of a single node in the system (Gb Ethernet = 110MB (APPROX))'''
BW_MAX = 115

BEST_CHUNK_READ_TIME = 0.0002


class BandwidthThreadControl(Thread):

    def __init__(self, log, initial_bw, server=None, method=None):

        Thread.__init__(self)
        self.log = log
        self.server = server
        self.method = method
        '''Last instant at which a bw calculation has been done'''
        self.stream_pipe_queue = Queue.Queue()
        ''' Timeout for read from queue'''
        self.timeout = 10
        '''This sleep value changes depending on the bw rate configured'''
        self.dynamic_sleep = 0.0
        '''Bytes transferred in the present bw control interval'''
        self.transferred_bytes_control = 0
        '''Number of data flow iterations within control interval'''
        self.number_of_iterations = 0
        '''Control periodically the enforcement of bw limits'''
        # self.control_process = eventlet.spawn(self.rate_control)
        '''Bandwidth limit per thread'''
        self.aggregated_bandwidth_limit = initial_bw
        self.bw_limits = dict()
        '''Monitoring information about bw consumption'''
        self.monitoring_info = dict()
        self.previous_monitoring_info = dict()
        '''To stop useless threads'''
        self.alive = True

        '''Dynamic sleep mean ** EXPERIMENTAL **'''
        max_iterations = int(
            round(
                ((self.aggregated_bandwidth_limit *
                  SAMPLE_CONTROL_INTERVAL) *
                 MB) /
                CHUNK_SIZE))
        read_chunk = BEST_CHUNK_READ_TIME * max_iterations
        self.calc_sleep = (
            (SAMPLE_CONTROL_INTERVAL -
             read_chunk) /
            max_iterations)
        self.dynamic_sleep_mean = self.calc_sleep
        self.total_sleep_mean = self.calc_sleep
        self.iters_mean = 1
        self.counter = 0
        self.bad_counter = 0

        self.rate_control = Thread(target=self.rate_control)
        self.rate_control.daemon = True
        self.rate_control.start()

    def add_stream_to_tenant(self, write_pipe, read_pipe, policy, device):
        pipe_tuple = (write_pipe, read_pipe, policy, device)
        self.stream_pipe_queue.put(pipe_tuple)
        '''Register the new requests on the tenant's thread for monitoring'''
        if policy not in self.monitoring_info:
            self.monitoring_info[policy] = dict()
            self.previous_monitoring_info[policy] = dict()
        if device not in self.monitoring_info[policy]:
            self.monitoring_info[policy][device] = 0
            self.previous_monitoring_info[policy][device] = 0
        '''Register new potential enforcement in bw limits dict'''
        if policy not in self.bw_limits:
            self.bw_limits[policy] = dict()
        if device not in self.bw_limits[policy]:
            self.bw_limits[policy][device] = BW_MAX

    def update_bw_limits(self, policy, device, limit):
        try:
            if limit == 0.0:
                limit = 1.0
            self.bw_limits[policy][device] = limit
            '''Update total aggregated bw limits that should be shared among
            devices'''
            updated_aggregated_bw_limit = 0.0
            for policy in self.bw_limits.keys():
                for device in self.bw_limits[policy]:
                    updated_aggregated_bw_limit += self.bw_limits[
                        policy][device]
            self.aggregated_bandwidth_limit = updated_aggregated_bw_limit

            max_iterations = int(
                round(
                    ((self.aggregated_bandwidth_limit *
                      SAMPLE_CONTROL_INTERVAL) *
                     MB) /
                    CHUNK_SIZE))
            read_chunk = BEST_CHUNK_READ_TIME * max_iterations
            self.calc_sleep = (
                (SAMPLE_CONTROL_INTERVAL - read_chunk) / max_iterations)

            self.dynamic_sleep_mean = self.calc_sleep
            self.dynamic_sleep = self.calc_sleep
            self.total_sleep_mean = self.calc_sleep
            self.iters_mean = 1

        except KeyError as e:
            print "Non-existing key in limits dict: " + str(e)

    def get_transferred_bw(self):
        '''Build a dictionary where entries are individual requests and the
        content is POLICY: DEVICE:BW'''
        monitoring_copy = copy.deepcopy(self.monitoring_info)
        diff_transferred_data = dict()
        for policy in monitoring_copy:
            diff_transferred_data[policy] = dict()
            for device in monitoring_copy[policy]:
                diff_transferred_data[policy][device] = monitoring_copy[policy][
                    device] - self.previous_monitoring_info[policy][device]
        self.previous_monitoring_info = monitoring_copy
        return diff_transferred_data

    def rate_control(self):
        while self.alive:
            f = open("/home/lab144/control.dat", 'a+')
            f.write("----------------\n")
            f.write("Queue: " + str(self.stream_pipe_queue.qsize()) + "\n")
            f.write("Iters: " + str(self.number_of_iterations) + "\n")

            '''Estimate the current transfer bw'''
            bandwidth_estimation = round(
                float(
                    self.transferred_bytes_control /
                    MB) /
                SAMPLE_CONTROL_INTERVAL,
                2)
            f.write(str(self.aggregated_bandwidth_limit) + "\n")
            f.write(str(bandwidth_estimation) + "\n")

            slo_deviation = abs(
                bandwidth_estimation -
                self.aggregated_bandwidth_limit)
            slo_deviation_percentage = (
                slo_deviation * 100) / self.aggregated_bandwidth_limit
            sleep_precentage = (
                self.dynamic_sleep * slo_deviation_percentage) / 100

            '''If we are under the expected bw, no sleep'''
            if bandwidth_estimation < self.aggregated_bandwidth_limit:
                new_dynamic_sleep = self.dynamic_sleep - sleep_precentage
            elif bandwidth_estimation > self.aggregated_bandwidth_limit:
                new_dynamic_sleep = self.dynamic_sleep + sleep_precentage

            f.write(
                "Antes: " +
                '{0:.6f}'.format(
                    round(
                        self.dynamic_sleep,
                        6)) +
                "\n")
            f.write(
                "Sleep: " +
                '{0:.6f}'.format(
                    round(
                        new_dynamic_sleep,
                        6)) +
                "\n")

            if slo_deviation < 1.5:
                self.iters_mean += 1
                self.total_sleep_mean += new_dynamic_sleep
                self.dynamic_sleep_mean = self.total_sleep_mean / self.iters_mean

            if new_dynamic_sleep / self.dynamic_sleep_mean < 1.2 and new_dynamic_sleep / \
                    self.dynamic_sleep_mean > 0.7:
                f.write(" --> GOOD <--\n")
                self.counter += 1
                self.bad_counter = 0
                self.dynamic_sleep = abs(new_dynamic_sleep)
            else:
                f.write(" --> BAAD <--\n")
                self.counter = 0
                self.bad_counter += 1

                if self.bad_counter > 10:
                    self.dynamic_sleep = new_dynamic_sleep
                    self.bad_counter = 0
                else:
                    self.dynamic_sleep = self.dynamic_sleep_mean

            f.write(
                "Mean: " +
                '{0:.6f}'.format(
                    round(
                        self.dynamic_sleep_mean,
                        6)) +
                "\n")
            f.write(
                "Calculated: " +
                '{0:.6f}'.format(
                    round(
                        self.calc_sleep,
                        6)) +
                "\n")
            f.write("Trusted Counter: " + str(self.counter) + "\n")
            f.write("Bad Counter: " + str(self.bad_counter) + "\n")
            f.close()

            '''Reset counters for new interval calculation'''
            self.transferred_bytes_control = 0
            self.number_of_iterations = 1
            '''Next time to calculate'''

            time.sleep(SAMPLE_CONTROL_INTERVAL)

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
                stream_data = self.stream_pipe_queue.get(timeout=self.timeout)

                (writer, reader, policy, device) = stream_data
                request_finished = False
                '''To share aggregated_bandwidth_limit among several requests
                to different devices,provide to each request a proportional
                number of chunk transfer operations'''

                proportional_device_bw = 32

                for token in range(proportional_device_bw):
                    try:
                        chunk = self._read_chunk(reader)
                        if chunk:
                            self._write_with_timeout(writer, chunk)

                            '''Account for the transferred data'''
                            self.transferred_bytes_control += len(chunk)
                            self.monitoring_info[policy][device] += len(chunk)
                            self.number_of_iterations += 1
                            '''Wait to achieve a certain bw rate
                               on this flow'''
                            time.sleep(max(0, self.dynamic_sleep))
                        else:
                            if hasattr(reader, 'close'):
                                reader.close()
                            writer.close()
                            request_finished = True
                            break

                    except IOError as e:
                        request_finished = True
                        self.log.info("Pipe error: " + str(e))
                        break
                    except Exception as e:
                        request_finished = True
                        self.log.info("An unknown error occurred during "
                                      "transfer: " + str(e))
                        break

                if not request_finished:
                    self.stream_pipe_queue.put(stream_data)

            except Queue.Empty:
                self.alive = False


class SSYNCBandwidthThreadControl(BandwidthThreadControl):

    def update_bw_limits(self, limit):
        self.aggregated_bandwidth_limit = limit
        self.iters_mean = 0
        self.total_sleep_mean = 0
        self.dynamic_sleep_mean = 0

    def run(self):
        while self.alive:
            try:
                stream_data = self.stream_pipe_queue.get(timeout=0)
                (writer, reader, p, device) = stream_data
                request_finished = False

                try:
                    chunk = reader.readline(CHUNK_SIZE)
                    writer.write(chunk)
                    writer.flush()

                    line = chunk.strip()

                    if line == ":UPDATES: END":
                        request_finished = True

                    if not request_finished:
                        if 'Content-Length' in line:
                            _, length = chunk.split(':', 1)

                        if not line:
                            ''' The next chunk is an object chunk '''
                            left = int(length)
                            while left > 0:
                                chunk = reader.read(min(left, CHUNK_SIZE))
                                writer.write(chunk)
                                writer.flush()
                                chunk_length = len(chunk)
                                left -= chunk_length
                                '''Account for the transferred data'''
                                self.transferred_bytes_control += chunk_length
                                self.monitoring_info[p][device] += chunk_length
                                self.number_of_iterations += 1
                                '''Wait to achieve a certain bw rate
                                   on this flow'''
                                time.sleep(max(0, self.dynamic_sleep))

                except IOError as e:
                    request_finished = True
                    self.log.info("Pipe error: " + str(e))
                    break
                except Exception as e:
                    request_finished = True
                    self.log.info("PAn unknown error occurred during "
                                  "transfer: " + str(e))
                    break

                if not request_finished:
                    self.stream_pipe_queue.put(stream_data)

            except Queue.Empty:
                self.alive = False


class IterLike(object):

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


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):  # @NoSelf
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(
                *args, **kwargs)
        return cls._instances[cls]


class BandwidthControl(object):
    __metaclass__ = Singleton

    def __init__(self, global_conf, filter_conf, logger):
        self.tenant_request_thread = {}
        self.tenant_response_thread = {}
        self.ssync_thread = {}
        self.producer_monitoring = None
        self.consumer_bw_assignments = None
        self.global_conf = global_conf
        self.log = logger

        # TODO: Load form filter_config
        self.global_conf['rabbit_host'] = 'controller'
        self.global_conf['rabbit_port'] = 5672
        self.global_conf['rabbit_username'] = 'openstack'
        self.global_conf['rabbit_password'] = 'rabbitmqastl1a4b4'

        self.global_conf['consumer_tag'] = 'bw_assignations'
        self.global_conf['routing_key_get'] = 'bwdifferentiation.get_bw_info'
        self.global_conf['routing_key_put'] = 'bwdifferentiation.put_bw_info'
        self.global_conf[
            'routing_key_ssync'] = 'bwdifferentiation.ssync_bw_info'
        self.global_conf['exchange_osinfo'] = 'amq.topic'
        self.global_conf['interval_osinfo'] = 0.2
        self.global_conf['bandwidth_control'] = 'proxy'
        self.global_conf['replication_one_per_dev'] = False
        # ------------------------------------------

        self.redis = redis.StrictRedis('controller', 6379, 0)

        self.server = self.global_conf.get('execution_server')

        rabbit_host = self.global_conf.get('rabbit_host')
        rabbit_port = int(self.global_conf.get('rabbit_port'))
        rabbit_user = self.global_conf.get('rabbit_username')
        rabbit_pass = self.global_conf.get('rabbit_password')

        self.ip = self.global_conf.get(
            'bind_ip') + ":" + self.global_conf.get('bind_port')

        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        parameters = pika.ConnectionParameters(host=rabbit_host,
                                               port=rabbit_port,
                                               credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)

        self._start_monitoring_producer()
        self._start_assignments_consumer()

    def execute(self, req_resp, crystal_iter, request_data):
        if isinstance(req_resp, Response):
            crystal_iter = self._register_response(
                request_data['account'], req_resp, crystal_iter)

        elif isinstance(req_resp, Request):
            crystal_iter = self._register_request(
                request_data['account'], req_resp, crystal_iter)

        return crystal_iter

    def _register_request(self, tenant, request, crystal_iter):
        r, w = os.pipe()
        write_pipe = os.fdopen(w, 'w')

        if crystal_iter:
            # Never enter here because this filter will be always the first
            read_pipe = crystal_iter
        else:
            read_pipe = request.environ['wsgi.input']

        if self.server == "proxy":
            container = request.environ['PATH_INFO'].split('/')[3]
            policy = int(
                request.environ[
                    'swift.container/' +
                    tenant +
                    '/' +
                    container]['storage_policy'])
        else:
            policy = int(
                request.environ['HTTP_X_BACKEND_STORAGE_POLICY_INDEX'])

        # device = request.environ['PATH_INFO'].split('/',2)[1]
        device = "sdb1"

        if tenant not in self.tenant_request_thread:
            self.log.info(
                "Crystal Filters - Bandwidth Differentiation Filter - Creating new "
                "PUT thread for tenant " + tenant)
            bw = self.redis.hgetall('bw:' + tenant)
            if str(policy) in bw:
                initial_bw = int(bw[str(policy)])
            else:
                initial_bw = BW_MAX
            thr = BandwidthThreadControl(
                self.log, initial_bw, self.server, 'PUT')
            self.tenant_request_thread[tenant] = thr
            thr.daemon = True
            thr.start()
        else:
            thr = self.tenant_request_thread[tenant]

        thr.add_stream_to_tenant(write_pipe, read_pipe, policy, device)

        return IterLike(r, 10)

    def _register_response(self, tenant, response, crystal_iter):
        r, w = os.pipe()
        write_pipe = os.fdopen(w, 'w')

        if crystal_iter:
            read_pipe = crystal_iter
        else:
            if self.server == 'proxy':
                read_pipe = response.app_iter
            else:
                read_pipe = response.app_iter._fp

        # device = response.headers['device']
        device = "sdb1"

        policy = int(response.environ['HTTP_X_BACKEND_STORAGE_POLICY_INDEX'])

        if tenant not in self.tenant_response_thread:
            self.log.info(
                "Crystal Filters - Bandwidth Differentiation Filter - Creating new "
                "GET thread for tenant " + tenant)
            bw = self.redis.hgetall('bw:' + tenant)
            if str(policy) in bw:
                initial_bw = int(bw[str(policy)])
            else:
                initial_bw = BW_MAX
            thr = BandwidthThreadControl(
                self.log, initial_bw, self.server, 'GET')
            self.tenant_response_thread[tenant] = thr
            thr.daemon = True
            thr.start()
        else:
            thr = self.tenant_response_thread[tenant]

        thr.add_stream_to_tenant(write_pipe, read_pipe, policy, device)

        return IterLike(r, 10)

    def register_ssync(self, request):
        r, w = os.pipe()
        write_pipe = os.fdopen(w, 'w')
        out_reader = os.fdopen(r, 'r')

        ssync_reader = request.environ["wsgi.input"]
        _, device, partition = request.environ["PATH_INFO"].split("/")
        source = request.environ['REMOTE_ADDR']

        if self.global_conf['replication_one_per_dev']:
            if not self.ssync_thread:
                thr = SSYNCBandwidthThreadControl(self.log)
                self.ssync_thread["source:" + source] = thr
                thr.add_stream_to_tenant(write_pipe, ssync_reader,
                                         partition, device)
                thr.start()
                request.environ['wsgi.input'] = out_reader
            else:
                # TODO: Return Response
                self.log.info(
                    "Crystal Filters - Bandwidth Differentiation Filter -"
                    " replication_one_per_device parameter is"
                    " setted to True: rejecting SSYNC /" + device + "/" + partition + " request")
        else:

            thr = SSYNCBandwidthThreadControl(self.log)
            self.ssync_thread["source:" + source] = thr
            thr.add_stream_to_tenant(write_pipe, ssync_reader,
                                     partition, device)
            thr.start()

            request.environ['wsgi.input'] = out_reader

    def _start_monitoring_producer(self):
        self.log.info(
            "Crystal Filters - Bandwidth Differentiation Filter - Strating monitoring "
            "producer")
        channel = self.connection.channel()
        thbw_get = Thread(target=self.bwinfo_threaded,
                          name='bwinfo_get_threaded',
                          args=('bwinfo_get_threaded', channel,
                                self.global_conf.get('interval_osinfo'),
                                self.global_conf.get('routing_key_get'),
                                self.tenant_response_thread, 'GET'))
        thbw_get.start()

        thbw_put = Thread(target=self.bwinfo_threaded,
                          name='bwinfo_put_threaded',
                          args=('bwinfo_put_threaded', channel,
                                self.global_conf.get('interval_osinfo'),
                                self.global_conf.get('routing_key_put'),
                                self.tenant_request_thread, 'PUT'))
        thbw_put.start()

        thbw_ssync = Thread(target=self.bwinfo_threaded,
                            name='bwinfo_ssync_threaded',
                            args=('bwinfo_ssync_threaded', channel,
                                  self.global_conf.get('interval_osinfo'),
                                  self.global_conf.get('routing_key_ssync'),
                                  self.ssync_thread, 'SSYNC'))
        thbw_ssync.start()

    def bwinfo_threaded(
            self,
            name,
            channel,
            interval,
            routing_key,
            threads,
            method):
        monitoring_data = dict()
        exchange = self.global_conf.get('exchange_osinfo')

        if method == 'SSYNC':
            get_monitoring_info = self._get_monitoring_info_ssync
        else:
            get_monitoring_info = self._get_monitoring_info

        while True:
            eventlet.sleep(interval)
            monitoring_info = get_monitoring_info(threads, interval)
            if monitoring_info:
                monitoring_data[self.ip] = monitoring_info
                channel.basic_publish(exchange=exchange,
                                      routing_key=routing_key,
                                      body=json.dumps(monitoring_data))

            '''Clean useless threads'''
            for tenant in threads.keys():
                if not threads[tenant].alive:
                    self.log.info(
                        "Crystal Filters - Bandwidth Differentiation Filter - Killing "
                        "thread " + tenant)
                    del threads[tenant]

    def _get_monitoring_info(self, threads, interval):
        '''Dictionary with bw estimates'''
        tenant_bw = dict()
        '''For all tenant-threads, get their bw consumption'''
        for tenant in threads:
            update = 0
            '''Structure: TENANT -> {POLICY: {DEVICE:Transferred Data}}'''
            thread_transferred_data = threads[tenant].get_transferred_bw()
            for policy in thread_transferred_data.keys():
                for device in thread_transferred_data[policy].keys():
                    thread_transferred_data[policy][device] /= float(interval)
                    if thread_transferred_data[policy][device] == 0.0:
                        del thread_transferred_data[policy][device]
                    else:
                        update += 1

            if update != 0:
                tenant_bw[tenant] = thread_transferred_data

        return tenant_bw

    def _get_monitoring_info_ssync(self, threads, interval):
        '''Dictionary with bw estimates'''
        tenant_bw = dict()
        '''For all tenant-threads, get their bw consumption'''
        for source in threads:
            update = 0
            '''Structure: SOURCE -> {DEVICE:Transferred Data}}'''
            thread_transferred_data = threads[source].get_transferred_bw()
            for partition in thread_transferred_data.keys():
                for device in thread_transferred_data[partition].keys():
                    thread_transferred_data[partition][
                        device] /= float(interval)
                    if thread_transferred_data[partition][device] == 0.0:
                        del thread_transferred_data[partition]
                    else:
                        update += 1

            if update != 0:
                tenant_bw[source] = dict()
                for partition in thread_transferred_data:
                    for device in thread_transferred_data[partition]:
                        tenant_bw[source][device] = thread_transferred_data[
                            partition][device]

        return tenant_bw

    def _start_assignments_consumer(self):
        self.log.info(
            "Crystal Filters - Bandwidth Differentiation Filter - Strating object "
            "storage assignments consumer")
        consumer_tag = self.global_conf.get('consumer_tag')
        queue_bw = consumer_tag + ":" + self.ip
        routing_key = "#." + self.ip.replace('.', '-').replace(':', '-') + ".#"

        channel = self.connection.channel()

        channel.queue_declare(queue=queue_bw)
        channel.exchange_declare(exchange=consumer_tag, type='topic')
        channel.queue_bind(
            exchange=consumer_tag,
            queue=queue_bw,
            routing_key=routing_key)
        channel.basic_consume(
            self._bw_assignations,
            queue=queue_bw,
            no_ack=True)

        thbw_assignation = Thread(target=channel.start_consuming)
        thbw_assignation.start()

    def _bw_assignations(self, ch, m, properties, body):
        _, account, method, policy, device, bw = body.split('/')
        if method == "GET":
            try:
                self.tenant_response_thread[account].update_bw_limits(
                    int(policy), device, float(bw))
            except KeyError:
                pass
        elif method == "PUT":
            try:
                self.tenant_request_thread[account].update_bw_limits(
                    int(policy), device, float(bw))
            except KeyError:
                pass
        elif method == "SSYNC":
            try:
                self.ssync_thread[account].update_bw_limits(float(bw))
            except KeyError:
                pass
