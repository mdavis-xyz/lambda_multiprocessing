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

`CICD` is for the GitHub Actions which run unit tests and integration tests.
You probably don't need to touch those.

## Design

When you `__enter__` the pool, it creates several `Child`s.
These contain the actual child `Process`es,
plus a duplex pipe to send tasks to the child and get results back.

The child process just waits for payloads to appear in the pipe.
It grabs the function and arguments from it, does the work,
catches any exception, then sends the exception or result back through the pipe.
Note that the function that the client gives to this library might return an Exception for some reason,
so we return either `[result, None]` or `[None, Exception]`, to differentiate.

To close everything up when we're done, the parent sends a payload with a different structure (`payload[-1] == True`)
and then the child will gracefully exit.

We keep a counter of how many tasks we've given to the child, minus how many results we've got back.
When assigning work, we give it to a child chosen randomly from the set of children whose counter value is smallest.
(i.e. shortest backlog)

When passing the question and answer to the child and back, we pass around a UUID.
This is because the client may submit two tasks with apply_async, then request the result for the second one,
before the first.
We can't assume that the next result coming back from the child is the one we want,
since each child can have a backlog of a few tasks.

Originally I passed a new pipe to the child for each task to process,
but this results in OSErrors from too many open files (i.e. too many pipes),
and passing pipes through pipes is unusually slow on low-memory Lambda functions for some reason.

Note that `multiprocessing.Queue` doesn't work in Lambda functions.
So we can't use that to distribute work amongst the children.
