Iris-BufferQueue
================

Iris-BufferQueue is a message queue implemented as a Java library. It is primarily intended for buffering data on local host before processing it asynchronously.

Iris-BufferQueue has the following characteristics:

- It's an *in-process* queue, although writing to the queue and consuming from it can happen in two different processes
- It's *persisted* on disk, preserving data across application restarts and crashes
- *Publishing* to the queue can happen in parallel from *multiple threads*; *no synchronisation* is required
- *Consumption* from the queue can only be done by a *single thread*
- *Consumption* can be happen in *batches*

These characteristics are explained in greater detail in [the documentation](design/characteristics.md).

### Get started

* [Quickstart](usage/quickstart.md)
* [Javadocs](//site/latest/javadocs/index.html)

### Learn more

* [Characteristics](design/characteristics.md)
* [Performance](design/performance.md)
* [Source Code](https://github.com/flipkart-incubator/Iris-BufferQueue)