
import contextlib
import errno
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time


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

    def _start(self):
        self._tempdir = tempfile.mkdtemp()
        self.host = 'localhost'

        # Allocation of unused ports is racy, so retry
        # until ports have been successfully acquired.
        while self._proc is None:
            http_port, raft_port = self._unused_ports(self.host, 2)
            self.http = (self.host, http_port)
            self.raft = (self.host, raft_port)
            with open(os.devnull, mode='wb', buffering=0) as devnull:
                filename = 'rqlited'
                try:
                    self._proc = subprocess.Popen([filename,
                        '-http-addr', '{}:{}'.format(*self.http),
                        '-raft-addr', '{}:{}'.format(*self.raft), self._tempdir],
                        stdout=devnull, stderr=devnull)
                except EnvironmentError as e:
                    if e.errno == errno.ENOENT and sys.version_info.major < 3:
                        # Add filename to clarify exception message.
                        e.filename = filename
                    raise

            while not self._test_port(*self.http) and self._proc.poll() is None:
                time.sleep(0.5)

            if self._proc.poll() is not None:
                self._proc = None

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._tempdir is not None:
            shutil.rmtree(self._tempdir)
            self._tempdir = None
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        return False
