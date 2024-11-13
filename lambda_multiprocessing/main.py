from multiprocessing import TimeoutError, Process, Pipe
from multiprocessing.connection import Connection
from typing import Any, Iterable, List, Dict, Tuple, Union, Optional, Callable
from uuid import uuid4, UUID
import random
import os
from time import time
from select import select
from itertools import repeat

# This is what we send down the pipe from parent to child
class Request:
    pass

class Task(Request):
    func: Callable
    args: List
    kwds: Dict
    id: UUID

    def __init__(self, func, args=(), kwds={}, id=None):
        self.func = func
        self.args = args
        self.kwds = kwds or {}
        self.id = id or uuid4()

class QuitSignal(Request):
    pass

class RunBatchSignal(Request):
    pass
    
# this is what we send back through the pipe from child to parent
class Response:
    id: UUID
    pass

class SuccessResponse(Response):
    result: Any
    def __init__(self, id: UUID, result: Any):
        self.result = result
        self.id = id

# processing the task raised an exception
# we save the exception in this object
class FailResponse(Response):
    exception: Exception
    def __init__(self, id: UUID, exception: Exception):
        self.id = id
        self.exception = exception

class Child:
    proc: Process

    # this is the number of items that we have sent to the child process
    # minus the number we have received back
    # includes items in the queue not processed,
    # items currently being processed
    # and items that have been processed by the child, but not read by the parent
    # does not include the termination command from parent to child
    queue_sz: int = 0

    # parent_conn.send(Request)  to give stuff to the child
    # parent_conn.recv() to get results back from child
    parent_conn: Connection
    child_conn: Connection

    response_cache: Dict[UUID, Tuple[Any, Exception]] = {}

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
        quit_signal = False
        while True:
            req_buf = []
            # first read in tasks until we get a pause/quit signal
            while True:
                request = self.child_conn.recv()
                if isinstance(request, Task):
                    req_buf.append(request)
                elif isinstance(request, QuitSignal):
                    # don't quit yet. Finish what's in the buffer.
                    quit_signal |= True
                    break # stop reading new stuff from the pipe
                elif isinstance(request, RunBatchSignal):
                    # stop reading from Pipe, process what's in the request buffer
                    break
            result_buf = []
            for req in req_buf:
                assert isinstance(req, Task)
                # process the result
                result = self._do_work(req)
                result_buf.append(result)

            # send all the results
            for result in result_buf:         
                self.child_conn.send(result)

            if quit_signal:
                break
        self.child_conn.close()

    # applies the function, catching any exception if it occurs
    def _do_work(self, task: Task) -> Response:
        try:
            result = task.func(*task.args, **task.kwds)
        except Exception as e:
            # how to handle KeyboardInterrupt?
            resp = FailResponse(id=task.id, exception=e)
        else:
            resp = SuccessResponse(id=task.id, result=result)
        return resp

    # this sends a task to the child
    # if as_batch=False, the child will start work on it immediately
    # If as_batch=True, the child will load this into it's local buffer
    # but won't start processing until we send a RunBatchSignal with self.run_batch()
    def submit(self, func, args=(), kwds=None, as_batch=False) -> 'AsyncResult':
        if self._closed:
            raise ValueError("Cannot submit tasks after closure")
        request = Task(func=func, args=args, kwds=kwds)

        self.parent_conn.send(request)
        if self.main_proc:
            self.child_conn.recv()
            ret = self._do_work(request)
            self.child_conn.send(ret)
        elif not as_batch:
            # treat this as a batch of 1
            self.run_batch()
        self.queue_sz += 1
        return AsyncResult(id=request.id, child=self)

    # non-blocking
    # Tells the child to start commencing work on all tasks sent up until now
    def run_batch(self):
        self.parent_conn.send(RunBatchSignal())

    # grab all results in the pipe from child to parent
    # save them to self.response_cache
    def flush_results(self):
        # watch out, when the other end is closed, a termination byte appears, so .poll() returns True
        while (not self.parent_conn.closed) and (self.queue_sz > 0) and self.parent_conn.poll(0):
            result = self.parent_conn.recv()
            id = result.id
            assert id not in self.response_cache
            self.response_cache[id] = result
            self.queue_sz -= 1

    # prevent new tasks from being submitted
    # but keep existing tasks running
    # should be idempotent
    def close(self):
        if not self._closed:
            if not self.main_proc:
                # send quit signal to child
                self.parent_conn.send(QuitSignal())
            else:
                # no child process to close
                self.flush_results()
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

        self.flush_results()
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
        self.response: Result = None

    # assume the result is in the self.child.response_cache
    # move it into self.response
    def _load(self):
        self.response = self.child.response_cache[self.id]
        del self.child.response_cache[self.id] # prevent memory leak

    # Return the result when it arrives.
    # If timeout is not None and the result does not arrive within timeout seconds
    # then multiprocessing.TimeoutError is raised.
    # If the remote call raised an exception then that exception will be reraised by get().
    # .get() must remember the result
    # and return it again multiple times
    # delete it from the Child.response_cache to avoid memory leak
    def get(self, timeout=None):
        if self.response is not None:
            if isinstance(self.response, SuccessResponse):
                return self.response.result
            elif isinstance(self.response, FailResponse):
                assert isinstance(self.response.exception, Exception)
                raise self.response.exception
        elif self.id in self.child.response_cache:
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
        if self.response is None:
            start_t = time()
            self.child.flush_results()
            # the result we want might not be the next result
            # it might be the 2nd or 3rd next
            while (self.id not in self.child.response_cache) and \
                  ((timeout is None) or (time() - timeout < start_t)):
                if timeout is None:
                    self.child.parent_conn.poll()
                else:
                    elapsed_so_far = time() - start_t
                    remaining = timeout - elapsed_so_far
                    self.child.parent_conn.poll(remaining)
                if self.child.parent_conn.poll(0):
                    self.child.flush_results()

    # Return whether the call has completed.
    def ready(self):
        self.child.flush_results()
        return self.response or (self.id in self.child.response_cache)

    # Return whether the call completed without raising an exception.
    # Will raise ValueError if the result is not ready.
    def successful(self):
        if self.response is None:
            if not self.ready():
                raise ValueError("Result is not ready")
            else:
                self._load()

        return isinstance(self.response, SuccessResponse)

# map_async and starmap_async return a single AsyncResult
# which is a list of actual results
# This class aggregates many AsyncResult into one
class AsyncResultList(AsyncResult):
    def __init__(self, child_results: List[AsyncResult]):
        self.child_results = child_results
        self.result: List[Union[Tuple[Any, None], Tuple[None, Exception]]] = None

    # assume the result is in the self.child.response_cache
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
    # delete it from the Child.response_cache to avoid memory leak
    def get(self, timeout=None):
        self.wait(timeout)
        assert self.ready()

        results = []
        for (i, c) in enumerate(self.child_results):
            try:
                result = c.get(0)
            except Exception:
                print(f"Exception raised for {i}th task out of {len(self.child_results)}")
                for c2 in self.child_results[i+1:]:
                    c2.child.flush_results()
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

        results = self._apply_batch_async(func, [args], [kwds])
        assert len(results) == 1
        return results[0]


    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> AsyncResult:
        return self.starmap_async(func, zip(iterable), chunksize, callback, error_callback)

    def map(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> List:
        return self.starmap(func, zip(iterable), chunksize, callback, error_callback)

    def starmap_async(self, func, iterable: Iterable[Iterable], chunksize=None, callback=None, error_callback=None) -> AsyncResult:
        if chunksize:
            raise NotImplementedError("Haven't implemented chunksizes. Infinite chunksize only.")
        if callback or error_callback:
            raise NotImplementedError("Haven't implemented callbacks")

        results = self._apply_batch_async(func, args_iterable=iterable, kwds_iterable=repeat({}))
        result = AsyncResultList(child_results=results)
        return result

    # like starmap, but has argument for keyword args
    # so apply_async can call this
    # (apply_async supports kwargs but starmap_async does not)
    def _apply_batch_async(self, func, args_iterable: Iterable[Iterable], kwds_iterable: Iterable[Dict]) -> List[AsyncResult]:
        if self._closed:
            raise ValueError("Pool already closed")

        for c in self.children:
            c.flush_results()

        results = []
        children_called = set()
        for (args, kwds) in zip(args_iterable, kwds_iterable):
            child = self._choose_child() # already flushed results
            children_called.add(child)
            result = child.submit(func, args, kwds, as_batch=True)
            results.append(result)

        for child in children_called:
            child.run_batch()

        return results
        

    # return the child with the shortest queue
    # if a tie, choose randomly
    # You should call c.flush_results() first before calling this
    def _choose_child(self) -> Child:
        min_q_sz = min(c.queue_sz for c in self.children)
        return random.choice([c for c in self.children if c.queue_sz <= min_q_sz])


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
                        child.flush_results()
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
