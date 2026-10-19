
import contextlib
import os
import shutil
import socket
import subprocess
import tempfile
import time
from http.client import HTTPConnection


RQLITED_PATH = os.environ.get("RQLITED_PATH", "rqlited")

class EphemeralRqlited(object):
    def __init__(self):
        self.host = None
        self.http = None
        self.raft = None
        self._tempdir = None
        self._proc = None

    @staticmethod
    def _unused_ports(host, count):
        sockets = []
        ports = []
        try:
            sockets.extend(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                for i in range(count))
            for s in sockets:
                s.bind((host, 0))
                ports.append(s.getsockname()[-1])
        finally:
            while sockets:
                sockets.pop().close()

        return ports

    @staticmethod
    def _test_port(host, port, timeout=None):
        try:
            with contextlib.closing(
                socket.create_connection((host, port), timeout=timeout)):
                return True
        except socket.error:
            return False

    @staticmethod
    def _test_readyz(host, port):
        try:
            with contextlib.closing(HTTPConnection(host, port=port)) as conn:
                conn.request("GET", "/readyz")
                return conn.getresponse().status == 200
        except Exception:
            return False

    # How many times to retry launching rqlited when a freshly chosen
    # port turns out to be taken by the time rqlited binds it.
    _MAX_ATTEMPTS = 10

    def _start(self):
        self.host = 'localhost'

        # Allocation of unused ports is racy, so retry until rqlited
        # has started and reports ready. Each attempt gets a fresh
        # data directory: a failed attempt leaves Raft state bound to
        # the old address, which the next attempt must not reuse.
        last_exit = None
        for _ in range(self._MAX_ATTEMPTS):
            self._tempdir = tempfile.mkdtemp()
            http_port, raft_port = self._unused_ports(self.host, 2)
            self.http = (self.host, http_port)
            self.raft = (self.host, raft_port)
            with open(os.devnull, mode='wb', buffering=0) as devnull:
                try:
                    self._proc = subprocess.Popen([RQLITED_PATH,
                        '-http-addr', '{}:{}'.format(*self.http),
                        '-raft-addr', '{}:{}'.format(*self.raft), self._tempdir],
                        stdout=devnull, stderr=devnull)
                except EnvironmentError:
                    self._remove_tempdir()
                    raise

            # Poll readiness while the process is alive. Previously the
            # readiness loop never checked the process, so if rqlited
            # died before serving /readyz this hung forever.
            while self._proc.poll() is None:
                if self._test_readyz(*self.http):
                    return
                time.sleep(0.5)

            last_exit = self._proc.returncode
            self._proc = None
            self._remove_tempdir()

        raise RuntimeError(
            "rqlited at {!r} failed to start after {} attempts "
            "(last exit status {})".format(
                RQLITED_PATH, self._MAX_ATTEMPTS, last_exit))

    def _remove_tempdir(self):
        if self._tempdir is not None:
            shutil.rmtree(self._tempdir, ignore_errors=True)
            self._tempdir = None

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # Stop the process before removing the directory it writes to.
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        self._remove_tempdir()
        return False
