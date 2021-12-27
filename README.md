# Multiprocessing in AWS Lambda

## The Problem

If you deploy Python code to an [AWS Lambda function](https://aws.amazon.com/lambda/),
the multiprocessing functions in the standard library such as `multiprocessing.Pool.map` will not work.

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
Amazon documented it [in this blog post].
However that example is very much tied to the work being done,
it doesn't have great error handling,
and is not structured in the way you'd expect when using the normal `multiprocessing` library.

The purpose of this library is to take the solution from that blog post,
and turn it into a drop-in replacement for `multiprocessing.Pool`.
This also includes unit testing, error handling etc, to match what you get from `multiprocessing.Pool`.

## Usage

Once you've imported `Pool`, it acts just like `multiprocessing.Pool`.
[Details here](https://docs.python.org/3/library/multiprocessing.html?highlight=multiprocessing%20python%20map%20pool#module-multiprocessing.pool).

```
from lammulti import Pool

def work(x):
    return x*x

with Pool() as p:
    results = p.map(work, range(5))
assert results == [x*x for x in range(5)]
```

Note that Lambda functions usually have only 1 vCPU.
If you allocate a lot or memory, they might get a second one.
Multiprocessing in a lambda is useful for IO-bound tasks
(e.g. uploading many files to S3, publishing many payloads to SNS etc).

You will not see any performance benefit compared to a `for` loop if you have a CPU-bound task.
(i.e. no upload/download).

## Limitations

When constructing the pool, initializer, initargs, maxtasksperchild and context have not been implemented.

For `*map*` functions,
callbacks and chunk sizes have not been implemented.

This library works by spawning several child processes,
and giving them tasks in a round-robin style.
If you have many quick tasks and one long task, then many quick tasks
(e.g. `Pool().map(time.sleep, [1,1,100,1,1])`)
then that long task will result in blocking.
If you have a use case like this, raise a GitHub issue.

## Concurrency Safety

Boto3 (the AWS SDK) is concurrency safe.
However the `client` and `session` objects should not be shared between processes or threads.
So do not pass those to or from the child processes.

`moto` (a library for unit-testing code that uses boto, by emulating most AWS services in memory)
is **not** concurrency safe.
So if you're unit testing using moto, pass `0` as the first argument to `Pool`,
and then all the work will actually be done in the main thread.
i.e. no multiprocessing at all
So you need an `if` statement to pass 0 or a positive integer based on whether this is unit testing or the real thing.
