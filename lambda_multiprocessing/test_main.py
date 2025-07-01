import unittest
import multiprocessing, multiprocessing.pool
from lambda_multiprocessing import Pool, TimeoutError, AsyncResult
from time import time, sleep
from typing import Tuple, Optional
from pathlib import Path
import os
import sys
from functools import cache

import boto3
from moto import mock_aws
from lambda_multiprocessing.timeout import TimeoutManager, TestTimeoutException

# add an overhead for duration when asserting the duration of child processes
# if other processes are hogging CPU, make this bigger
delta = 0.1

SEC_PER_MIN = 60

# some simple functions to run inside the child process
def square(x):
    return x*x

def fail(x):
    assert False, "deliberate fail"

def sum_two(a, b):
    return a + b

def divide(a, b):
    return a / b

def return_args_kwargs(*args, **kwargs):
    return {'args': args, 'kwargs': kwargs}

def return_with_sleep(x, delay=0.3):
    sleep(delay)
    return x

def _raise(ex: Optional[Exception]):
    if ex:
        raise ex

class ExceptionA(Exception):
    pass
class ExceptionB(Exception):
    pass

class TestStdLib(unittest.TestCase):
    @unittest.skip('Need to set up to remove /dev/shm')
    def test_standard_library(self):
        with self.assertRaises(OSError):
            with multiprocessing.Pool() as p:
                ret = p.map(square, range(5))

# add assertDuration
class TestCase(unittest.TestCase):

    max_timeout = SEC_PER_MIN*2
    timeout_mgr = TimeoutManager(seconds=max_timeout)

    def setUp(self):
        self.timeout_mgr.start()

    def tearDown(self):
        self.timeout_mgr.stop()

    # use like
    # with self.assertDuration(1, 2):
    #   something
    # to assert that something takes between 1 to 2 seconds to run
    # If the task takes forever, this assertion will not be raised
    # For a potential eternal task, use timeout.TimeoutManager
    def assertDuration(self, min_t=None, max_t=None):
        class AssertDuration:
            def __init__(self, test: unittest.TestCase):
                self.test = test
            def __enter__(self):
                self.start_t = time()

            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type is None:
                    end_t = time()
                    duration = end_t - self.start_t
                    if min_t is not None:
                        self.test.assertGreaterEqual(duration, min_t, f"Took less than {min_t}s to run")
                    if max_t is not None:
                        self.test.assertLessEqual(duration, max_t, f"Took more than {max_t}s to run")
                return False

        return AssertDuration(self)



class TestAssertDuration(TestCase):

    def test(self):
        # check that self.assertDurationWorks
        with self.assertRaises(AssertionError):
            with self.assertDuration(min_t=1):
                pass

        with self.assertRaises(AssertionError):
            with self.assertDuration(max_t=0.5):
                sleep(1)

        t = 0.5
        with self.assertDuration(min_t=t-delta, max_t=t + delta):
            sleep(t)

class TestApply(TestCase):
    pool_generator = Pool

    def test_simple(self):
        args = range(5)
        with self.pool_generator() as p:
            actual = [p.apply(square, (x,)) for x in args]
        with multiprocessing.Pool() as p:
            expected = [p.apply(square, (x,)) for x in args]
        self.assertEqual(actual, expected)

    def test_two_args(self):
        with self.pool_generator() as p:
            actual = p.apply(sum_two, (3, 4))
            self.assertEqual(actual, sum_two(3, 4))

    def test_kwargs(self):
        with self.pool_generator() as p:
            actual = p.apply(return_args_kwargs, (1, 2), {'x': 'X', 'y': 'Y'})
            expected = {
                'args': (1,2),
                'kwargs': {
                    'x': 'X',
                    'y': 'Y'
                }
            }
            self.assertEqual(actual, expected)

# rerun all the tests with stdlib
# to confirm we have the same behavior
class TestApplyStdLib(TestApply):
    pool_generator = multiprocessing.Pool

class TestApplyAsync(TestCase):
    pool_generator = Pool

    def test_result(self):
        with self.pool_generator() as p:
            r = p.apply_async(square, (2,))
            result = r.get(2)
            self.assertEqual(result, square(2))

    def test_twice(self):
        args = range(5)
        # now try twice with the same pool
        with self.pool_generator() as p:
            ret = [p.apply_async(square, (x,)) for x in args]
            ret.extend(p.apply_async(square, (x,)) for x in args)

        self.assertEqual([square(x) for x in args] * 2, [square(x) for x in args] * 2)

    def test_time(self):
        # test the timing to confirm it's in parallel
        n = 4
        with self.pool_generator(n) as p:
            with self.assertDuration(min_t=n-1, max_t=(n-1)+delta):
                with self.assertDuration(max_t=1):
                    ret = [p.apply_async(sleep, (r,)) for r in range(n)]
                ret = [r.get(n*2) for r in ret]

    @unittest.skip("Standard library doesn't handle this well, unsure whether to do the same or not")
    def test_unclean_exit(self):
        # .get after __exit__, but process finishes before __exit__
        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with self.pool_generator() as p:
                r = p.apply_async(square, (t,))
                sleep(1)
            r.get()

        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with self.pool_generator() as p:
                r = p.apply_async(square, (t,))
                sleep(1)
            r.get(1)

        # __exit__ before process finishes
        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with self.pool_generator() as p:
                r = p.apply_async(sleep, (t,))
            r.get(t+1) # .get() with arg after __exit__

        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with self.pool_generator() as p:
                r = p.apply_async(sleep, (t,))
            r.get() # .get() without arg after __exit__

    def test_get_not_ready_a(self):
        t = 2
        with self.pool_generator() as p:
            r = p.apply_async(sleep, (t,))
            with self.assertRaises(multiprocessing.TimeoutError):
                r.get(t-1) # result not ready get

    def test_get_not_ready_b(self):
        t = 2
        with self.pool_generator() as p:
            # check same exception exists from main
            r = p.apply_async(sleep, (t,))
            with self.assertRaises(TimeoutError):
                r.get(t-1)

    def test_get_not_ready_c(self):
        t = 2
        with self.pool_generator() as p:
            r = p.apply_async(sleep, (t,))
            sleep(1)
            self.assertFalse(r.ready())
            sleep(t)
            self.assertTrue(r.ready())

    def test_wait(self):
        with self.pool_generator() as p:
            r = p.apply_async(square, (1,))
            with self.assertDuration(max_t=delta):
                r.wait()
            self.assertTrue(r.ready())
            ret = r.get(0)
            self.assertEqual(ret, square(1))

            # now check when not ready
            r = p.apply_async(sleep, (5,))
            self.assertFalse(r.ready())
            with self.assertDuration(min_t=2, max_t=2.1):
                r.wait(2)
            self.assertFalse(r.ready())

            # now check with a wait longer than the task
            with self.assertDuration(min_t=1, max_t=1.1):
                r = p.apply_async(sleep, (1,))
                self.assertFalse(r.ready())
                r.wait(2)
            self.assertTrue(r.ready())
            ret = r.get(0)

    def test_get_twice(self):
        with self.pool_generator() as p:
            r = p.apply_async(square, (2,))
            self.assertEqual(r.get(), square(2))
            self.assertEqual(r.get(), square(2))

    def test_successful(self):
        with self.pool_generator() as p:
            r = p.apply_async(square, (1,))
            sleep(delta)
            self.assertTrue(r.successful())

            r = p.apply_async(sleep, (1,))
            with self.assertRaises(ValueError):
                r.successful()

            sleep(1.2)
            self.assertTrue(r.successful())


            r = p.apply_async(fail, (1,))
            sleep(delta)
            self.assertFalse(r.successful())

    def test_two_args(self):
        with self.pool_generator() as p:
            ret = p.apply_async(sum_two, (1, 2))
            ret.wait()
            self.assertEqual(ret.get(), sum_two(1,2))

    def test_kwargs(self):
        with self.pool_generator() as p:
            actual = p.apply_async(return_args_kwargs, (1, 2), {'x': 'X', 'y': 'Y'}).get()
            expected = {
                'args': (1,2),
                'kwargs': {
                    'x': 'X',
                    'y': 'Y'
                }
            }
            self.assertEqual(actual, expected)

    def test_error_handling(self):
        with self.assertRaises(AssertionError):
            with self.pool_generator() as p:
                r = p.apply_async(fail, (1,))
                r.get()

# retrun tests with standard library
# to confirm our behavior matches theirs
class TestApplyAsyncStdLib(TestApplyAsync):
    pool_generator = multiprocessing.Pool

class TestMap(TestCase):
    pool_generator = Pool
    
    def test_simple(self):
        args = range(5)
        with self.pool_generator() as p:
            actual = p.map(square, args)
        self.assertIsInstance(actual, list)
        expected = [square(x) for x in args]
        self.assertEqual(expected, actual)
        self.assertIsInstance(actual, list)

    def test_duration(self):
        n = 2
        with self.pool_generator(n) as p:
            with self.assertDuration(min_t=(n-1)-delta, max_t=(n+1)+delta):
                p.map(sleep, range(n))

    def test_error_handling(self):
        with self.pool_generator() as p:
            with self.assertRaises(AssertionError):
                p.map(fail, range(2))

    @unittest.skip('Need to implement chunking to fix this')
    def test_long_iter(self):
        with self.pool_generator() as p:
            p.map(square, range(10**3))

    def test_without_with(self):
        p = self.pool_generator(3)
        ret = p.map(square, [1,2])
        self.assertEqual(ret, [1,2*2])
        p.close()
        p.join()

class TestMapStdLib(TestMap):
    pool_generator = multiprocessing.Pool

class TestMapAsync(TestCase):
    pool_generator = Pool

    def test_simple(self):
        num_payloads = 5
        args = range(num_payloads)
        for num_procs in [(num_payloads+1), (num_payloads-1)]:
            with self.pool_generator(num_procs) as p:
                actual = p.map_async(square, args)
                self.assertIsInstance(actual, (AsyncResult, multiprocessing.pool.AsyncResult))
                results = actual.get()
            self.assertEqual(results, [square(e) for e in args])

    def test_duration(self):
        sleep_duration = 0.5
        n_procs = 2
        num_tasks_per_proc = 3
        expected_wall_time = sleep_duration * num_tasks_per_proc
        with self.pool_generator(n_procs) as p:
            with self.assertDuration(min_t=expected_wall_time-delta, max_t=expected_wall_time+delta):
                with self.assertDuration(max_t=delta):
                    results = p.map_async(sleep, [sleep_duration] * (num_tasks_per_proc * n_procs))
                results.get()
       

    def test_error_handling(self):
        with self.pool_generator() as p:
            r = p.map_async(fail, range(2))
            with self.assertRaises(AssertionError):
                r.get()

    def test_multi_error(self):
        # standard library can raise either error

        with self.assertRaises((ExceptionA, ExceptionB)):
            with self.pool_generator() as p:
                r = p.map_async(_raise, (None, ExceptionA("Task 1"), None, ExceptionB("Task 3")))
                r.get()

class TestMapAsyncStdLib(TestMapAsync):
    pool_generator = multiprocessing.Pool

class TestStarmap(TestCase):
    pool_generator = Pool

    def test(self):
        with self.pool_generator() as p:
            actual = p.starmap(sum_two, [(1,2), (3,4)])
        expected = [(1+2), (3+4)]
        self.assertEqual(actual, expected)

    def test_error_handling(self):
        with self.pool_generator() as p:
            with self.assertRaises(ZeroDivisionError):
                p.starmap(divide, [(1,2), (3,0)])

class TestStarmapStdLib(TestStarmap):
    pool_generator = multiprocessing.Pool

class TestStarmapAsync(TestCase):
    pool_generator = Pool

    def test(self):
        with self.pool_generator() as p:
            response = p.starmap_async(sum_two, [(1,2), (3,4)])
            self.assertIsInstance(response, (AsyncResult, multiprocessing.pool.AsyncResult))
            actual = response.get()
        expected = [(1+2), (3+4)]
        self.assertEqual(actual, expected)

    def test_error_handling(self):
        with self.pool_generator() as p:
            results = p.starmap_async(divide, [(1,2), (3,0)])
            with self.assertRaises(ZeroDivisionError):
                results.get()



class TestStarmapAsyncStdLib(TestStarmapAsync):
    pool_generator = multiprocessing.Pool

class TestExit(TestCase):
    # only test this with our library,
    # not the standard library
    # because the standard library has a bug
    # https://github.com/python/cpython/issues/79659

    # test that the implicit __exit__
    # waits for child process to finish
    def test_exit(self):
        t = 1
        with Pool() as p:
            t1 = time()
            r = p.apply_async(sleep, (t,))
        t2 = time()
        self.assertLessEqual(abs((t2-t1)-t), delta)

class TestTidyUp(TestCase):
    pool_generator = Pool

    # test that .close() stops new submisssions
    # but does not halt existing num_processes
    # nor wait for them to finish
    def test_close(self):
        t = 1
        with self.pool_generator() as p:
            r = p.apply_async(sleep, (t,))
            with self.assertDuration(max_t=delta):
                p.close()
            with self.assertDuration(min_t=t-delta, max_t=t+delta):
                p.join()
            pass # makes traceback from __exit__ clearer

    def test_submit_after_close(self):
        with self.pool_generator() as p:
            p.close()
            with self.assertRaises(ValueError):
                p.apply_async(square, (1,))

    # test that .terminate does not
    # wait for child process to finish
    def test_terminate(self):
        with self.assertDuration(max_t=delta):
            with self.pool_generator() as p:
                r = p.apply_async(sleep, (1,))
                t1 = time()
                p.terminate()
                t2 = time()
                self.assertLessEqual(t2-t1, delta)

    def test_submit_after_terminate(self):
        with self.pool_generator() as p:
            p.terminate()
            with self.assertRaises(ValueError):
                p.apply_async(square, (1,))

class TestTidyUpStdLib(TestTidyUp):
    pool_generator = multiprocessing.Pool

class TestDeadlock(TestCase):

    # test this issue:
    # https://github.com/mdavis-xyz/lambda_multiprocessing/issues/17
    def test_map_deadlock(self):

        child_sleep = 0.01
        num_payloads = 6

        # use standard library to start with
        # and to measure a 'normal' duration
        # (the time spend passing data between processes is longer than the sleep
        #  inside the worker)
        expected_duration = child_sleep * num_payloads
        data = [self.generate_big_data() for _ in range(num_payloads)]
        start_t = time()

        with multiprocessing.Pool(processes=1) as p:
            p.map(return_with_sleep, data)
        
        end_t = time()
        stdlib_duration = end_t - start_t

        # this timeout manager doesn't work
        # need to run the parent inside another process/thread?
        # now our one
        data = [self.generate_big_data() for _ in range(num_payloads)]
        with Pool(processes=1) as p:
            with TimeoutManager(stdlib_duration*2, "This Library's map deadlocked"):
                try:
                    p.map(return_with_sleep, data)
                except TestTimeoutException:
                    p.terminate()
                    raise

    def test_map_async_deadlock(self):

        child_sleep = 0.4
        num_payloads = 3
        data = [self.generate_big_data() for _ in range(num_payloads)]

        # use standard library to start with
        # and to measure a 'normal' duration
        # (the time spend passing data between processes is longer than the sleep
        #  inside the worker)
        expected_duration = child_sleep * num_payloads
        start_t = time()
        with multiprocessing.Pool(processes=1) as p:
            results = p.map_async(return_with_sleep, data)
            results.get()
        end_t = time()
        stdlib_duration = end_t - start_t

        # now our one
        with Pool(processes=1) as p:
            with TimeoutManager(stdlib_duration*2, "This Library's map_async deadlocked"):
                try:
                    results = p.map_async(return_with_sleep, data)
                    results.get()
                except TestTimeoutException:
                    p.terminate()
                    raise
                
    # test that map_async returns immediately
    # even when there are multiple tasks per child
    # with payloads bigger than the buffer
    def test_nonblocking(self):
        sleep_duration = 2
        n_procs = 2
        num_tasks_per_proc = 3
        expected_wall_time = sleep_duration * num_tasks_per_proc

        args = [(self.generate_big_data(), sleep_duration)] * (num_tasks_per_proc * n_procs)
        with Pool(n_procs) as p:
            try:
                with TimeoutManager(sleep_duration * num_tasks_per_proc * 2, "This Library's map_async deadlocked"):
                    with self.assertDuration(max_t=sleep_duration*0.5):
                        results = p.map_async(return_with_sleep, args)
                results.get(expected_wall_time*1.5)
            except Exception:
                p.terminate()
                raise

    @classmethod
    @cache
    def generate_big_data(cls, sz=2**24) -> bytes:
        return 'x' * sz


# must be a global method to be pickleable
def upload(args: Tuple[str, str, bytes]):
    (bucket_name, key, data) = args
    client = boto3.client('s3')
    client.put_object(Bucket=bucket_name, Key=key, Body=data)


class TestMoto(TestCase):
    def test_main_proc(self):
        t = 1
        with Pool(0) as p:
            with self.assertDuration(min_t=t-delta, max_t=t+delta):
                r = p.apply_async(sleep, (t,))
                pass # makes traceback from __exit__ easier to read

    @mock_aws
    def test_moto(self):
        bucket_name = 'mybucket'
        key = 'my-file'
        data = b"123"
        region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        client = boto3.client('s3', region_name=region)
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region
            },
        )
        # upload in a different thread
        # when we create the other process,
        # everything in moto is duplicated (e.g. existing bucket)
        # so the function should execute correctly
        # but it will upload an object to mocked S3 in the child process
        # so that file won't exist in the parent process
        with Pool() as p:
            p.apply(upload, ((bucket_name,key, data),))
        with self.assertRaises(client.exceptions.NoSuchKey):
            client.get_object(Bucket=bucket_name, Key=key)['Body'].read()

        # now try again with 0 processes
        with Pool(0) as p:
            p.apply(upload, ((bucket_name,key, data),))

        ret = client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
        self.assertEqual(ret, data)

class TestSlow(TestCase):
    @unittest.skip('Very slow')
    def test_memory_leak(self):
        for i in range(10**2):
            with Pool() as p:
                for j in range(10**2):
                    p.map(square, range(10**3))

if __name__ == '__main__':
    unittest.main()
