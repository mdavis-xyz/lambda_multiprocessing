# lambda_multiprocessing - Multiprocessing in AWS Lambda

This library is for doing multiprocessing in AWS Lambda in python.

(This is unrelated to inline lambda functions such as `f = lambda x: x*x`.
 That's a different kind of lambda function.)

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
Amazon documented it [in this blog post](https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/).
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
from lambda_multiprocessing import Pool

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
(i.e. no upload/download, no sleeping and no disk read/write).

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

For this reason, if you have a long iterable of payloads,
you may get `OSError: [Errno 24] Too many open files`.
Raise a GitHub issue if this impacts you.

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
