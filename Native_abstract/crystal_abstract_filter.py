from eventlet import Timeout


class AbstractFilter(object):

    def __init__(self, global_conf, filter_conf, logger):
        self.logger = logger
        self.global_conf = global_conf
        self.filter_conf = filter_conf

    def execute(self, req_resp, crystal_iter, requets_data):
        method = requets_data['method']

        if method == 'get':
            crystal_iter = self._get_object(req_resp, crystal_iter)

        elif method == 'put':
            crystal_iter = self._put_object(req_resp, crystal_iter)

        return crystal_iter

    def _get_object(self, req_resp, crystal_iter):
        return IterLike(crystal_iter, 10, self.filter_put)

    def _put_object(self, request, crystal_iter):
        return IterLike(crystal_iter, 10, self.filter_get)

    def filter_put(self, chunk):
        return chunk

    def filter_get(self, chunk):
        return chunk


class IterLike(object):
    def __init__(self, obj_data, timeout, filter_method):
        self.closed = False
        self.obj_data = obj_data
        self.timeout = timeout
        self.buf = b''

        self.filter = filter_method

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = self.obj_data.read(size)
                chunk = self.filter(chunk)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise

        return chunk

    def next(self, size=64 * 1024):
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
        self.obj_data.close()
        self.cahed_object.close()
        self.closed = True

    def __del__(self):
        self.close()
