from multiprocessing import TimeoutError, Process, Pipe
from multiprocessing.connection import Connection
from typing import Any, Iterable, List
import os

class AsyncResult:
    def __init__(self, Connection):
        self.conn = Connection
        self.result_ready = False
        self.result: Tuple[Any, Exception] = None

    # grab the result, and save it
    # assume it's ready
    def _load(self):
        assert not self.result_ready
        self.result = self.conn.recv()
        self.result_ready = True
        self.conn.close()

    # .get() must remember the result
    # and return it again multiple times
    def get(self, timeout=None):
        if self.result_ready:
            (result, ex) = self.result
            if ex:
                # which approach two use?
                # first is what the standard library does
                # I think the second is clearer?
                raise ex
                # raise RuntimeError("Function inside child process failed") from ex
            else:
                return result
        else:
            self.wait(timeout)
            if not self.ready():
                raise TimeoutError("result not ready")
            else:
                self._load()
                return self.get()

    def wait(self, timeout=None):
        if not self.result_ready:
            self.conn.poll(timeout)

    def ready(self):
        # if the other end closed the connection before sending anything
        # .poll() returns True, because there's a termination byte in the pipe
        return self.result_ready | self.conn.poll(0)

    def successful(self):
        if not self.result_ready:
            if not self.ready():
                raise ValueError("Result is not ready")
            else:
                self._load()

        return self.result[1] is None

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

    def __enter__(self):
        self._closed = False

        # list of Pipes to write commands to the child process
        # and receive results
        # see docs for self.spin for payload details
        self.pipes: List[Connection] = []

        # list of child processes
        # they sit and wait for commands via command_pipes
        self.child_procs: List[Process] = []

        for n in range(self.num_processes):
            recv_conn, send_conn  = Pipe(False)
            self.pipes.append(send_conn)
            p = Process(target=self.spin, args=(recv_conn,))
            p.start()
            self.child_procs.append(p)

        # give the next job to this child
        # round robin style
        self.next_child = 0

        self.per_task_pipes: List[Connection] = []

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
            # tell each child process to exit gracefully
            # once they finish their existing class
            for pipe in self.pipes:
                pipe.send([None, True])
                pipe.close()

            # keep track of closure,
            # so subsequent task submissions are rejected
            self._closed = True

    # wait for existing tasks to finish
    def join(self):
        assert self._closed, "Must close before joining"
        for p in self.child_procs:
            try:
                p.join()
            except ValueError as e:
                # .join() has probably been called multiple times
                # so the process has already been closed
                pass
            finally:
                p.close()

        for p in self.per_task_pipes:
            # probably already closed (closing twice is ok)
            # won't be closed yet if the AsyncResult.get() was never called sucessfully
            p.close()

    # terminate child processes without waiting for them to finish
    def terminate(self):
        for p in self.child_procs:
            try:
                a = p.is_alive()
            except ValueError:
                # already closed
                # .is_alive seems to raise ValueError not return False if dead
                pass
            else:
                if a:
                    try:
                        p.close()
                    except ValueError:
                        p.terminate()
        for pipe in self.pipes:
            pipe.close()
        self._closed |= True
    # each child process runs in this
    # a while loop waiting for payloads from the Pipe
    # [(result_conn, func, args, kwds), None] -> call func(args, *kwds)
    #                         and send the return back through the result_pipe pipe
    #                         [ret, None] if func returned ret
    #                         [None, err] if func raised exception err
    # [None, True] -> exit gracefully
    def spin(self, conn: Connection) -> None:
        while True:
            (job, quit_signal) = conn.recv()
            if quit_signal:
                break
            else:
                (result_conn, func, args, kwds) = job
                try:
                    result = func(*args, **kwds)
                except Exception as e:
                    # how to handle KeyboardInterrupt?
                    ret = (None, e)
                else:
                    ret = (result, None)
                result_conn.send(ret)
        conn.close()

    def apply(self, func, args=(), kwds={}):
        ret = self.apply_async(func, args, kwds)
        return ret.get()

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None) -> AsyncResult:
        if callback:
            raise NotImplementedError("callback not implemented")
        if error_callback:
            raise NotImplementedError("error_callback not implemented")

        if self._closed:
            raise ValueError("Pool already closed")


        if self.num_processes:
            # send the payload to the child
            parent_conn, child_conn  = Pipe(False)

            self.pipes[self.next_child].send([[child_conn, func, args, kwds], None])
            self.next_child = (self.next_child + 1) % self.num_processes # round robin
            self.per_task_pipes.append(parent_conn) # to be garbage collected later
            return AsyncResult(parent_conn)
        else:
            # run in the main thread
            # for when we unit test using moto
            # send the result into a Pipe and back to the same process
            # so we can re-use the same AsyncResult approach
            recv_conn, send_conn  = Pipe(False)
            try:
                # do the actual work in the main thread
                result = func(*args, **kwds)
            except Exception as e:
                ret = (None, e)
            else:
                ret = (result, None)
            send_conn.send(ret)
            send_conn.close()
            self.per_task_pipes.append(recv_conn) # to be garbage collected later
            return AsyncResult(recv_conn)

    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> List[AsyncResult]:
        return self.starmap_async(func, zip(iterable), chunksize, callback, error_callback)

    def map(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> List:
        return self.starmap(func, zip(iterable), chunksize, callback, error_callback)

    def starmap_async(self, func, iterable: Iterable[Iterable], chunksize=None, callback=None, error_callback=None) -> List[AsyncResult]:
        if chunksize:
            raise NotImplementedError("Haven't implemented chunksizes. Infinite chunksize only.")
        if callback or error_callback:
            raise NotImplementedError("Haven't implemented callbacks")
        return [self.apply_async(func, args) for args in iterable]

    def starmap(self, func, iterable: Iterable[Iterable], chunksize=None, callback=None, error_callback=None) -> List[Any]:
        results = self.starmap_async(func, iterable, chunksize, callback, error_callback)
        return [r.get() for r in results]
