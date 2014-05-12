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

These characteristics are explained in greater detail in [the documentation](http://flipkart-incubator.github.io/Iris-BufferQueue/#/design/characteristics).


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
	  <version>0.1</version>
	</dependency>

#### Download

You can download the jar and find all the dependencies on [Clojars](https://clojars.org/com.flipkart.iris/bufferqueue).

### Create an instance

    File file = new File("test.ibq");
    if (!file.exists()) {
        int maxDataLength = 4 * 1024; // max size of data that can be written to the queue
        long numMessages = 1000000; // maximum number of unconsumed messages that can be kept in the queue
        MappedBufferQueueFactory.format(file, maxDataLength, numMessages);
    }
    BufferQueue bufferQueue = MappedBufferQueueFactory.getInstance(file);

### Publish

Let's publish a simple message to the queue.

    byte[] data = "Hello world!".getBytes();

##### High-level API

    bufferQueue.publish(data);

##### Low-level API

    BufferQueueEntry entry = bufferQueue.next().orNull();
    if (entry != null) {
    	try {
		    entry.set(data);
		}
		finally {
		    entry.markPublished();
		}
	}
	else {
		System.out.println("Queue full, cannot write message");
	}

It is important that the `markPublished()` call is done within a `finally` block to ensure that it is always made.

### Consuming from the queue

##### Simple API

    BufferQueueEntry entry = bufferQueue.consume().orNull());
    if (entry != null) {
	    try {
	        byte[] data = entry.get();
	        System.out.println(data);
	    }
	    finally {
	        entry.markConsumed();
	    }
	}
	else {
		System.out.prinltn("Nothing to consume");
	}

It is important that the `markConsumed()` call is done within a `finally` block to ensure that it is always made.

##### Batch API

	int batchSize = 100;
    List<BufferQueueEntry> entries = bufferQueue.consume(batchSize);
    if (entries.size() > 0) {
	    for (BufferQueueEntry entry : entries) {
	        try {
	            byte[] data = entry.get();
	            System.out.println(data);
	        }
	        finally {
	            entry.markConsumed();
	        }
	    }
	}
	else {
		System.out.prinltn("Nothing to consume");
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
