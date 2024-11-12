from multiprocessing import TimeoutError, Process, Pipe
from multiprocessing.connection import Connection
from typing import Any, Iterable, List, Dict, Tuple, Union, Optional
from uuid import uuid4, UUID
import random
import os
from time import time
from select import select
import signal

class Child:
    proc: Process

    # this is the number of items that we have sent to the child process
    # minus the number we have received back
    # includes items in the queue not processed,
    # items currently being processed
    # and items that have been processed by the child, but not read by the parent
    # does not include the termination command from parent to child
    queue_sz: int = 0

    # parent_conn.send()  to give stuff to the child
    # parent_conn.recv() to get results back from child
    parent_conn: Connection
    child_conn: Connection

    result_cache: Dict[UUID, Tuple[Any, Exception]] = {}

    _closed: bool = False

    # if True, do the work in the main process
    # but present the same interface
    # and still send stuff through the pipes (to verify they're pickleable)
    # this is so we can unit test with moto
    main_proc: bool

    def __init__(self, main_proc=False):
        self.parent_conn, self.child_conn = Pipe(duplex=True)
        self.main_proc = main_proc
        if not main_proc:
            self.proc = Process(target=self.spin)
            self.proc.start()

    # each child process runs in this
    # a while loop waiting for payloads from the self.child_conn
    # [(id, func, args, kwds), None] -> call func(args, *kwds)
    #                         and send the return back through the self.child_conn pipe
    #                         {id: (ret, None)} if func returned ret
    #                         {id: (None, err)} if func raised exception err
    # [None, True] -> exit gracefully (write nothing to the pipe)
    def spin(self) -> None:
        while True:
            (job, quit_signal) = self.child_conn.recv()
            if quit_signal:
                break
            else:
                (id, func, args, kwds) = job
                result = self._do_work(id, func, args, kwds)
                self.child_conn.send(result)
        self.child_conn.close()

    def _do_work(self, id, func, args, kwds) -> Union[Tuple[Any, None], Tuple[None, Exception]]:
        try:
            ret = {id: (func(*args, **kwds), None)}
        except Exception as e:
            # how to handle KeyboardInterrupt?
            ret = {id: (None, e)}
        assert isinstance(list(ret.keys())[0], UUID)
        return ret

    def submit(self, func, args=(), kwds=None) -> 'AsyncResult':
        if self._closed:
            raise ValueError("Cannot submit tasks after closure")
        if kwds is None:
            kwds = {}
        id = uuid4()
        self.parent_conn.send([(id, func, args, kwds), None])
        if self.main_proc:
            self.child_conn.recv()
            ret = self._do_work(id, func, args, kwds)
            self.child_conn.send(ret)
        self.queue_sz += 1
        return AsyncResult(id=id, child=self)

    # grab all results in the pipe from child to parent
    # save them to self.result_cache
    def flush(self):
        # watch out, when the other end is closed, a termination byte appears, so .poll() returns True
        while (not self.parent_conn.closed) and (self.queue_sz > 0) and self.parent_conn.poll(0):
            result = self.parent_conn.recv()
            assert isinstance(list(result.keys())[0], UUID)
            self.result_cache.update(result)
            self.queue_sz -= 1

    # prevent new tasks from being submitted
    # but keep existing tasks running
    # should be idempotent
    def close(self):
        if not self._closed:
            if not self.main_proc:
                # send quit signal to child
                self.parent_conn.send([None, True])
            else:
                # no child process to close
                self.flush()
                self.child_conn.close()

            # keep track of closure,
            # so subsequent task submissions are rejected
            self._closed = True

    # after closing
    # wait for existing tasks to finish
    # should be idempotent
    def join(self):
        assert self._closed, "Must close before joining"
        if not self.main_proc:
            try:
                self.proc.join()
            except ValueError as e:
                # .join() has probably been called multiple times
                # so the process has already been closed
                pass
            finally:
                self.proc.close()

        self.flush()
        self.parent_conn.close()


    # terminate child processes without waiting for them to finish
    # should be idempotent
    def terminate(self):
        if not self.main_proc:
            try:
                a = self.proc.is_alive()
            except ValueError:
                # already closed
                # .is_alive seems to raise ValueError not return False if dead
                pass
            else:
                if a:
                    try:
                        self.proc.close()
                    except ValueError:
                        self.proc.terminate()
        self.parent_conn.close()
        self.child_conn.close()
        self._closed |= True

    def __del__(self):
        self.terminate()


class AsyncResult:
    def __init__(self, id: UUID, child: Child):
        assert isinstance(id, UUID)
        self.id = id
        self.child = child
        self.result: Union[Tuple[Any, None], Tuple[None, Exception]] = None

    # assume the result is in the self.child.result_cache
    # move it into self.result
    def _load(self):
        self.result = self.child.result_cache[self.id]
        del self.child.result_cache[self.id] # prevent memory leak

    # Return the result when it arrives.
    # If timeout is not None and the result does not arrive within timeout seconds
    # then multiprocessing.TimeoutError is raised.
    # If the remote call raised an exception then that exception will be reraised by get().
    # .get() must remember the result
    # and return it again multiple times
    # delete it from the Child.result_cache to avoid memory leak
    def get(self, timeout=None):
        if self.result is not None:
            (response, ex) = self.result
            if ex:
                raise ex
            else:
                return response
        elif self.id in self.child.result_cache:
            self._load()
            return self.get(0)
        else:
            self.wait(timeout)
            if not self.ready():
                raise TimeoutError("result not ready")
            else:
                return self.get(0)

    # Wait until the result is available or until timeout seconds pass.
    def wait(self, timeout=None):
        start_t = time()
        if self.result is None:
            self.child.flush()
            # the result we want might not be the next result
            # it might be the 2nd or 3rd next
            while (self.id not in self.child.result_cache) and \
                  ((timeout is None) or (time() - timeout < start_t)):
                if timeout is None:
                    self.child.parent_conn.poll()
                else:
                    elapsed_so_far = time() - start_t
                    remaining = timeout - elapsed_so_far
                    self.child.parent_conn.poll(remaining)
                if self.child.parent_conn.poll(0):
                    self.child.flush()

    # Return whether the call has completed.
    def ready(self):
        self.child.flush()
        return self.result or (self.id in self.child.result_cache)

    # Return whether the call completed without raising an exception.
    # Will raise ValueError if the result is not ready.
    def successful(self):
        if self.result is None:
            if not self.ready():
                raise ValueError("Result is not ready")
            else:
                self._load()

        return self.result[1] is None

# map_async and starmap_async return a single AsyncResult
# which is a list of actual results
# This class aggregates many AsyncResult into one
class AsyncResultList(AsyncResult):
    def __init__(self, child_results: List[AsyncResult]):
        self.child_results = child_results
        self.result: List[Union[Tuple[Any, None], Tuple[None, Exception]]] = None

    # assume the result is in the self.child.result_cache
    # move it into self.result
    def _load(self):
        for c in self.child_results:
            c._load()

    # Return the result when it arrives.
    # If timeout is not None and the result does not arrive within timeout seconds
    # then multiprocessing.TimeoutError is raised.
    # If the remote call raised an exception then that exception will be reraised by get().
    # .get() must remember the result
    # and return it again multiple times
    # delete it from the Child.result_cache to avoid memory leak
    def get(self, timeout=None):
        if timeout:
            end_t = time() + timeout
        else:
            end_t = None

        results = []
        for (i, c) in enumerate(self.child_results):
            # Consider cumulative timeout
            if timeout is not None:
                timeout_remaining = end_t - time()
            else:
                timeout_remaining = None

            try:
                result = c.get(timeout=timeout_remaining)
            except Exception:
                print(f"Exception raised for {i}th task out of {len(self.child_results)}")
                # terminate remaining children
                for c2 in self.child_results[i+1:]:
                    c2.child.terminate()
                raise

            results.append(result)
        return results

    # Wait until the result is available or until timeout seconds pass.
    def wait(self, timeout=None):
        
        if timeout:
            end_t = time() + timeout
        else:
            end_t = None
        for c in self.child_results:
            # Consider cumulative timeout
            if timeout is not None:
                timeout_remaining = end_t - time()
            else:
                timeout_remaining = None

            c.wait(timeout_remaining)

    # Return whether the call has completed.
    def ready(self):
        return all(c.ready() for c in self.child_results)

    # Return whether the call completed without raising an exception.
    # Will raise ValueError if the result is not ready.
    def successful(self):
        return all(c.successful() for c in self.child_results)

class Pool:
    def __init__(self, processes=None, initializer=None, initargs=None, maxtasksperchild=None, context=None):
        if processes is None:
            self.num_processes = os.cpu_count()
        else:
            if processes < 0:
                raise ValueError("processes must be a positive integer")
            self.num_processes = processes


        if initializer:
            raise NotImplementedError("initializer not implemented")

        if initargs:
            raise NotImplementedError("initargs not implemented")

        if maxtasksperchild:
            raise NotImplementedError("maxtasksperchild not implemented")

        if context:
            raise NotImplementedError("context not implemented")

        self._closed = False

        if self.num_processes > 0:
            self.children = [Child() for _ in range(self.num_processes)]
        else:
            # create one 'child' which will just do work in the main thread
            self.children = [Child(main_proc=True)]


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.join()
        self.terminate()

    def __del__(self):
        self.terminate()

    # prevent new tasks from being submitted
    # but keep existing tasks running
    def close(self):
        if not self._closed:
            for c in self.children:
                c.close()
            self._closed |= True

    # wait for existing tasks to finish
    def join(self):
        assert self._closed, "Must close before joining"
        for c in self.children:
            c.join()

    # terminate child processes without waiting for them to finish
    def terminate(self):
        for c in self.children:
            c.terminate()
        self._closed |= True

    def apply(self, func, args=(), kwds=None):
        ret = self.apply_async(func, args, kwds)
        return ret.get()

    def apply_async(self, func, args=(), kwds=None, callback=None, error_callback=None) -> AsyncResult:
        if callback:
            raise NotImplementedError("callback not implemented")
        if error_callback:
            raise NotImplementedError("error_callback not implemented")

        if self._closed:
            raise ValueError("Pool already closed")
        if kwds is None:
            kwds = {}


        # choose the first idle process if there is one
        # if not, choose the process with the shortest queue
        for c in self.children:
            c.flush()
        min_q_sz = min(c.queue_sz for c in self.children)
        c = random.choice([c for c in self.children if c.queue_sz <= min_q_sz])
        return c.submit(func, args, kwds)

    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> AsyncResult:
        return self.starmap_async(func, zip(iterable), chunksize, callback, error_callback)

    def map(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> List:
        return self.starmap(func, zip(iterable), chunksize, callback, error_callback)

    def starmap_async(self, func, iterable: Iterable[Iterable], chunksize=None, callback=None, error_callback=None) -> AsyncResult:
        if chunksize:
            raise NotImplementedError("Haven't implemented chunksizes. Infinite chunksize only.")
        if callback or error_callback:
            raise NotImplementedError("Haven't implemented callbacks")
        results = [self.apply_async(func, args) for args in iterable]

        # aggregate into one result
        result = AsyncResultList(child_results=results)

        return result

    def starmap(self, func, iterable: Iterable[Iterable], chunksize=None, callback=None, error_callback=None) -> List[Any]:
        if chunksize:
            raise NotImplementedError("chunksize not implemented")
        if callback:
            raise NotImplementedError("callback not implemented")
        if error_callback:
            raise NotImplementedError("error_callback not implemented")

        idle_children = set(self.children)
        ids = []
        pending_results: Dict[UUID, AsyncResult] = {}

        for args in iterable:
            if not idle_children:
                # wait for a child to become idle
                # by waiting for any of the pipes from children to become readable
                ready, _, _ = select([c.parent_conn for c in self.children], [], [])

                # at least one child is idle. 
                # check all children, read their last result from the pipe
                # then issue the new task
                for child in self.children:
                    if child.parent_conn in ready:
                        assert child.parent_conn.poll()
                        child.flush()
                        idle_children.add(child)
                

            child = idle_children.pop()
            async_result = child.submit(func, args)
            pending_results[async_result.id] = async_result
            ids.append(async_result.id)


        if len(idle_children) < len(self.children):
            # if at least one child is still working
            # wait with select
            ready, _, _ = select([c.parent_conn for c in self.children if c not in idle_children], [], [])

        # get all the results
        # re-arranging the order
        results = []
        for (i, id) in enumerate(ids):
            result = pending_results[id].get()
            results.append(result)

        return results

    def imap(self, func, iterable, chunksize=None):
        raise NotImplementedError("Only normal apply, map, starmap and their async equivilents have been implemented")

    def imap_unordered(self, func, iterable, chunksize=None):
        raise NotImplementedError("Only normal apply, map, starmap and their async equivilents have been implemented")
