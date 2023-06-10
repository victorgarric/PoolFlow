import PoolFlow as pf
import time
import unittest


class TestDynamicPool(unittest.TestCase):

    @staticmethod
    def pre(a):
        time.sleep(2)

    @staticmethod
    def post(a, b):
        time.sleep(2)

    @staticmethod
    def run_fn(txt):
        proc = pf.LimitedProcess('pythonw', 2)
        proc.launch()

    def test_main(self):
        self.pool = pf.DynamicPool()
        self.pool.start()
        for i in range(1, 4):
            self.pool.submit(self.run_fn, (i,), 2, (self.pre, (i,)), (self.post, (i, i ** 2)))
        self.pool.end()


class TestStaticPool(unittest.TestCase):

    @staticmethod
    def run_fn(a):
        time.sleep(a)

    def test_main(self):
        self.pool = pf.StaticPool(self.run_fn, ((1,), (2,), (3,)), 2, override_max_value=5)
        self.pool.start()
        self.pool.end()
