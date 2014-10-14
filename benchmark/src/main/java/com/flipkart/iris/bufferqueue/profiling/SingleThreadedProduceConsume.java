package com.flipkart.iris.bufferqueue.profiling;

import com.flipkart.iris.bufferqueue.BufferQueue;
import com.flipkart.iris.bufferqueue.BufferQueueEntry;
import com.flipkart.iris.bufferqueue.mmapped.MappedBufferQueue;
import com.google.common.base.Optional;

import java.io.File;
import java.io.IOException;

public class SingleThreadedProduceConsume {
    public static void main(String[] args) throws IOException {
        String file = "/tmp/test.mbq";// args[0];
        MappedBufferQueue bufferQueue = new MappedBufferQueue.Builder(new File(file))
                .build();


        BufferQueue.Consumer consumer = bufferQueue.consumer();
        BufferQueue.Publisher publisher = bufferQueue.publisher();

        byte[] msg = "Hello World!".getBytes();

        System.gc();
        System.out.println("Starting loop");
        bufferQueue.printBufferSkeleton("after startup");

        boolean publish = publisher.publish(msg);
        if (!publish) {
            System.out.println("Publish failed");
        }
        Optional<? extends BufferQueueEntry> entry = consumer.peek();
        if (entry.isPresent()) {
            entry.get().markConsumed();
        }
        else {
            System.out.println("Got an empty object");
        }

        System.exit(0);

//        while (true) {
//            boolean publish = publisher.publish(msg);
//            if (!publish) {
//                System.out.println("Publish failed");
//            }
//
//            Optional<? extends BufferQueueEntry> entry = consumer.peek();
//            if (entry.isPresent()) {
//                entry.get().markConsumed();
//            }
//            else {
//                System.out.println("Got an empty object");
//            }
//        }
    }
}
