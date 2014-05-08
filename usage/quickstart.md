Quickstart
----------

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

Take a look at the [Javadocs for MappedBufferQueueFactory](//site/latest/javadocs/com/flipkart/iris/bufferqueue/mmapped/MappedBufferQueueFactory.html) for more details.

### Publish

Let's publish a simple message to the queue.

    byte[] data = "Hello world!".getBytes();

##### High-level API

    bufferQueue.publish(data);

See the Javadocs for more details: [BufferQueue#publish(byte[])](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueue.html#publish(byte[]\)).

##### Low-level API

There are no practical reasons to use the low-level publishing API as of now. But the plan is to enable more efficient writing

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

It is important that the `markPublished()` call is done within a `finally` block to ensure that it is always made. See the Javadocs for more details:
[BufferQueue#next()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueue.html#next(\)),
[BufferQueueEntry#set()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueueEntry.html#set(\)),
[BufferQueueEntry#markPublished()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueueEntry.html#markPublished(\))

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

It is important that the `markConsumed()` call is done within a `finally` block to ensure that it is always made. See the Javadocs for more details:
[BufferQueue#consume()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueue.html#consume(\)),
[BufferQueueEntry#get()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueueEntry.html#get(\)),
[BufferQueueEntry#markConsumed()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueueEntry.html#markConsumed(\)).

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

It is important that the `markConsumed()` call is done within a `finally` block to ensure that it is always made. See the Javadocs for more details:
[BufferQueue#consume(int)](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueue.html#consume(int\)),
[BufferQueueEntry#get()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueueEntry.html#get(\)),
[BufferQueueEntry#markConsumed()](//site/latest/javadocs/com/flipkart/iris/bufferqueue/BufferQueueEntry.html#markConsumed(\)).
