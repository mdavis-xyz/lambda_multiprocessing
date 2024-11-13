# lambda_multiprocessing - Multiprocessing in AWS Lambda

This library is for doing multiprocessing in AWS Lambda in python.

(This is unrelated to inline lambda functions such as `f = lambda x: x*x`.
 That's a different kind of lambda function.)

## The Problem

If you deploy Python code to an [AWS Lambda function](https://aws.amazon.com/lambda/),
the multiprocessing functions in the standard library such as [`multiprocessing.Pool.map`](https://docs.python.org/3/library/multiprocessing.html?highlight=multiprocessing%20python%20map%20pool#multiprocessing.pool.Pool.map) will not work.

For example:

```
from multiprocessing import Pool
def func(x):
    return x*x
args = [1,2,3]
with Pool() as p:
    result = p.map(func, args)
```

will give you:

```
OSError: [Errno 38] Function not implemented
```

This is because AWS Lambda functions are very bare bones,
and have no shared memory device (`/dev/shm`).



## The Solution

There is a workaround using `Pipe`s and `Process`es.
Amazon documented it [in this blog post](https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/).
However that example is very much tied to the work being done,
it doesn't have great error handling,
and is not structured in the way you'd expect when using the normal `multiprocessing` library.

The purpose of this library is to take the solution from that blog post,
and turn it into a drop-in replacement for `multiprocessing.Pool`.
This also includes unit testing, error handling etc, to match what you get from `multiprocessing.Pool`.

## Usage

Install with:

```
pip install lambda_multiprocessing
```

Once you've imported `Pool`, it acts just like `multiprocessing.Pool`.
[Details here](https://docs.python.org/3/library/multiprocessing.html?highlight=multiprocessing%20python%20map%20pool#module-multiprocessing.pool).

```
from lambda_multiprocessing import Pool

def work(x):
    return x*x

with Pool() as p:
    results = p.map(work, range(5))
assert results == [x*x for x in range(5)]
```

Note that Lambda functions usually have only 2 vCPUs.
If you allocate a lot or memory you get a few more.
(e.g. 3 at 5120MB, 6 at 10240MB)
The performance benefit you get from multiprocessing CPU-bound tasks is equal to the number of CPUs, minus overhead.
(e.g. double speed for multiprocessing with 2 vCPUs)
You can get bigger performance benefits for IO-bound tasks.
(e.g. uploading many files to S3, publishing many payloads to SNS etc).

## Limitations

When constructing the pool, initializer, initargs, maxtasksperchild and context have not been implemented.

For `*map*` functions,
callbacks and chunk sizes have not been implemented.

`imap` and `imap_unordered` have not been implemented.

If you need any of these things implemented, raise an issue or a PR in github.

## Concurrency Safety

Boto3 (the AWS SDK) is concurrency safe.
However the `client` and `session` objects should not be shared between processes or threads.
So do not pass those to or from the child processes.

`moto` (a library for unit-testing code that uses boto, by emulating most AWS services in memory)
is **not** concurrency safe.
So if you're unit testing using moto, pass `0` as the first argument to `Pool`,
and then all the work will actually be done in the main thread.
i.e. no multiprocessing at all.
So you need an `if` statement to pass 0 or a positive integer based on whether this is unit testing or the real thing.

## Development

This library has no dependencies.
The unit tests depend on `boto3` and `moto`.

```
pip install -r lambda_multiprocessing/requirements_test.txt
```

Then you can run the unit tests with:

```
python3 -m unittest
```

The tests themselves are defined in `lambda_multiprocessing/test_main.py`.

`CICD` is for the GitHub Actions which run the unit tests and integration tests.
You probably don't need to touch those.


## Design

When you `__enter__` the pool, it creates several `Child`s.
These contain the actual child `Process`es,
plus a duplex pipe to send `Task`s to the child and get `Response`s back.

The child process just waits for payloads to appear in the pipe.
It grabs the function and arguments from it, does the work,
catches any exception, then sends the exception or result back through the pipe.
Note that the arguments and return functions to this function could be anything.
(It's even possible that the function _returns_ an Exception, instead of throwing one.)

To close everything up when we're done, the parent sends a different subclass of `Request`, which is `QuitSignal`. Upon receiving this, the child will gracefully exit.

We keep a counter of how many tasks we've given to the child, minus how many results we've got back.
When assigning work, we give it to a child chosen randomly from the set of children whose counter value is smallest.
(i.e. shortest backlog)

When passing the question and answer to the child and back, we pass around a UUID.
This is because the client may submit two tasks with apply_async, then request the result for the second one, before the first.
We can't assume that the next result coming back from the child is the one we want,
since each child can have a backlog of a few tasks.

Originally I passed a new pipe to the child for each task to process,
but this results in OSErrors from too many open files (i.e. too many pipes),
and passing pipes through pipes is unusually slow on low-memory Lambda functions for some reason.

Note that `multiprocessing.Queue` doesn't work in Lambda functions.
So we can't use that to distribute work amongst the children.

### Deadlock

Note that we must be careful about a particular deadlocking scenario,
described in [this issue](https://github.com/mdavis-xyz/lambda_multiprocessing/issues/17#issuecomment-2468560515)

Writes to `Pipe`s are usually non-blocking. However if you're writing something large
(>90kB, IIRC) the Pipe's buffer will fill up. The writer will block,
waiting for the reader at the other end to start reading.

The situation which previously occured was:

* parent sends a task to the child
* child reads the task from the pipe, and starts working on it
* parent immediately sends the next task, which blocks because the object is larger than the buffer
* child tries sending the result from the first task, which blocks because the result is larger than the buffer

In this situation both processes are mid-way through writing something, and won't continue until the other starts reading from the other pipe.
Solutions like having the parent/child check if there's data to receive before sending data won't work because those two steps are not atomic. (I did not want to introduce locks, for performance reasons.)

The solution is that the child will read all available `Task` payloads sent from the parent, into a local buffer, without commencing work on them. Then once it receives a `RunBatchSignal`, it stops reading anything else from the parent, and starts work on the tasks in the buffer. By batching tasks in this way, we can prevent the deadlock, and also ensure that the `*async` functions are non-blocking for large payloads.

## Release Notes

### 1.0

* Breaking change: `map_async` and `starmap_async` return a single `AsyncResult`, whose `.get()` returns a list. Previously they returns a list of `AsyncResult`, but this does not match `multiprocessing.Pool`.
* Bugfix: Fixed deadlock for large request/response case [\#17](https://github.com/mdavis-xyz/lambda_multiprocessing/issues/17)
