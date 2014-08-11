Iris-BufferQueue
================

Iris-BufferQueue is a message queue implemented as a Java library. It is primarily intended for buffering data on local
host before processing it asynchronously.

Iris-BufferQueue has the following characteristics:

- It's an *in-process* queue, although writing to the queue and consuming from it can happen in two different processes
- It's *persisted* on disk, preserving data across application restarts and crashes
- *Publishing* to the queue can happen in parallel from *multiple threads*; *no synchronisation* is required
- *Consumption* from the queue can only be done by a *single thread*
- *Consumption* can be happen in *batches*

These characteristics are explained in greater detail in [the documentation](docs/characteristics.md).


Usage
-----

### Get the Jar

#### Maven

Add the following repository to the 'repositories' section of your pom.xml

    <repository>
      <id>clojars</id>
      <name>Clojars repository</name>
      <url>https://clojars.org/repo</url>
    </repository>

And add the following dependency to the 'dependencies' section of your pom.xml.

	<dependency>
	  <groupId>com.flipkart.iris</groupId>
	  <artifactId>bufferqueue</artifactId>
	  <version>0.2-SNAPSHOT</version>
	</dependency>

#### Download

You can download the jar and find all the dependencies on [Clojars](https://clojars.org/com.flipkart.iris/bufferqueue).

### Create an instance

    File dir = new File("/tmp/bqtest");    // directory to create mapping files in
    int blockSize = 1024;                  // the mapped files are divided into "blocks", this is the size of each block
    long fileSize = 512 * 1024 * 1024;     // 512 MB
    int maxFiles = Integer.MAX_VALUE;      // maximum number of files to create, no impact as of now
    MappedDirBufferQueue bufferQueue = new MappedDirBufferQueue(dir, blockSize, fileSize, maxFiles);

### Publishing to the queue

    byte[] data = "Hello world!".getBytes();
    BufferQueue.Publisher publisher = bufferQueue.publisher();

##### High-level API

    if (publisher.publish(data)) {
        // success
    }
    else {
        // failure
    }

##### Low-level API

    BufferQueueEntry entry = publisher.claimFor(data).orNull();
    if (entry != null) {
    	try {
		    entry.set(data);
		}
		finally {
		    entry.markPublishedUnconsumed();
		}
	}
	else {
		System.out.println("Queue full, cannot write message");
	}

It is important that the `markPublishedUnconsumed()` call is done within a `finally` block to ensure that it is always made.

### Consuming from the queue

    BufferQueue.Consumer consumer = bufferQueue.consumer();

#### Unsafe APIs

When you use the unsafe APIs, you stand a chance to lose some data — if your code calls `consume` but dies before
processing the returned data, that data would have to be deemed lost i.e. it will not be available for processing again.

##### Unsafe Simple API

    byte[] data = consumer.consume().orNull());
    if (data != null) {
        // process data
	}
	else {
		// nothing to consume; maybe sleep for some time?
	}

##### Unsafe Batch API

	int batchSize = 100;
    List<byte[]> dataList = consumer.consume(batchSize);
    if (dataList.size() > 0) {
	    for (byte[] data : dataList) {
            // process data
	    }
	}
	else {
		// nothing to consumer; maybe sleep for some time?
	}	

#### Safe APIs

With safe APIs you'll have to explicitly confirm that you have processed any returned data; only after such a
confirmation will the data be discarded. But note: given that consumption from BufferQueue is sequential, if there is
even one particular entry whose processing is taking a long time, it'll stall all the consumption.

##### Safe Simple API

    BufferQueueEntry entry = consumer.peek().orNull());
    if (entry != null) {
	    try {
	        byte[] data = entry.get();
	        // process data
	    }
	    finally {
	        entry.markConsumed();
	    }
	}
	else {
		// nothing to consume; maybe sleep for some time?
	}

It is important that the `markConsumed()` call is done within a `finally` block to ensure that it is always made.

##### Safe Batch API

	int batchSize = 100;
    List<BufferQueueEntry> entries = consumer.peek(batchSize);
    if (entries.size() > 0) {
	    for (BufferQueueEntry entry : entries) {
	        try {
	            byte[] data = entry.get();
	            // process data
	        }
	        finally {
	            entry.markConsumed();
	        }
	    }
	}
	else {
		// nothing to consumer; maybe sleep for some time?
	}	

It is important that the `markConsumed()` call is done within a `finally` block to ensure that it is always made.

Documentation
-------------

TODO: Point to detailed design, usage and API docs.

Contribution, Bugs and Feedback
-------------------------------

For bugs, questions and discussions please use the [Github Issues](https://github.com/flipkart-incubator/Iris-BufferQueue/issues).

Please follow the [contribution guidelines](https://github.com/flipkart-incubator/Iris-BufferQueue/blob/master/CONTRIBUTING.md) when submitting pull requests.


LICENSE
-------

Copyright 2014 Flipkart Internet Pvt. Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
