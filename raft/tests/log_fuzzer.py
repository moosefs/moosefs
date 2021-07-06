from cffi import FFI

import subprocess
import unittest

from hypothesis import given
from hypothesis.strategies import lists, just, integers, one_of
from distutils.spawn import find_executable


class Libraft(object):
    def __init__(self):
        ffi = FFI()
        ffi.set_source(
            "tests",
            """
            """,
            sources="""
                src/raft_log.c
                src/raft_server.c
                src/raft_server_properties.c
                src/raft_node.c
                """.split(),
            include_dirs=["include"],
            )
        library = ffi.compile()

        self.ffi = ffi = FFI()
        self.lib = ffi.dlopen(library)

        def load(fname):
            gcc = find_executable('gcc')
            if gcc is None:
                print("#Error, unable to locate gcc")
                return 1
            elif not gcc.startswith('/usr/bin') and not gcc.startswith('/bin'):
                print("#Error, find_executable(gcc)=", gcc)
                print("#Unable to locate gcc under /bin or /usr/bin.")
                return 1
            else:
                return subprocess.check_output([gcc, "-E", fname])

        ffi.cdef(load('include/raft.h'))
        ffi.cdef(load('include/raft_log.h'))


commands = one_of(
    just('append'),
    just('poll'),
    integers(min_value=1, max_value=10),
)


class Log(object):
    def __init__(self):
        self.entries = []
        self.base = 0

    def append(self, ety):
        self.entries.append(ety)

    def poll(self):
        self.base += 1
        return self.entries.pop(0)

    def delete(self, idx):
        idx -= 1
        if idx < self.base:
            idx = self.base
        idx = max(idx - self.base, 0)
        del self.entries[idx:]

    def count(self):
        return len(self.entries)


class CoreTestCase(unittest.TestCase):
    def setUp(self):
        super(CoreTestCase, self).setUp()
        self.r = Libraft()

    @given(lists(commands))
    def test_sanity_check(self, commands):
        r = self.r.lib

        unique_id = 1
        l = r.log_alloc(1)

        log = Log()

        for cmd in commands:
            if cmd == 'append':
                entry = self.r.ffi.new('raft_entry_t*')
                entry.id = unique_id
                unique_id += 1

                ret = r.log_append_entry(l, entry)
                assert ret == 0 # nosec

                log.append(entry)

            elif cmd == 'poll':
                entry_ptr = self.r.ffi.new('void**')

                if log.entries:
                    ret = r.log_poll(l, entry_ptr)
                    assert ret == 0 # nosec

                    ety_expected = log.poll()
                    ety_actual = self.r.ffi.cast('raft_entry_t**', entry_ptr)[0]
                    assert ety_actual.id == ety_expected.id # nosec

            elif isinstance(cmd, int):
                if log.entries:
                    log.delete(cmd)
                    ret = r.log_delete(l, cmd)
                    assert ret == 0 # nosec

            else:
                assert False # nosec

            self.assertEqual(r.log_count(l), log.count())


if __name__ == '__main__':
    unittest.main()
