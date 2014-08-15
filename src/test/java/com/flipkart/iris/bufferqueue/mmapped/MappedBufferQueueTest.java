package com.flipkart.iris.bufferqueue.mmapped;

import com.flipkart.iris.bufferqueue.BufferQueue;
import com.google.common.base.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MappedBufferQueueTest {
    private static final byte[] MSG1 = "hello world".getBytes();
    private static final String MSG_PREFIX = "hello world";

    File file;
    MappedBufferQueue bufferQueue;

    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("bufferqueue-test", Long.toString(System.nanoTime()));
        file.delete();
    }

    @After
    public void tearDown() throws Exception {
        if (bufferQueue != null) {
            bufferQueue.close();
        }
        if (!file.delete()) {
            System.err.println("Couldn't delete temporary directory that was created: " + file);
        }
    }

    @Test
    public void testSimplePublishConsume() throws Exception {
        bufferQueue = new MappedBufferQueue.Builder(this.file)
                .formatIfNotExists(10000, 1024)
                .headerSyncInterval(1)
                .build();
        BufferQueue.Publisher publisher = bufferQueue.publisher();
        BufferQueue.Consumer consumer = bufferQueue.consumer();

        publisher.publish(MSG1);
        assertArrayEquals(MSG1, consumer.consume().get());
        assertEquals(Optional.<byte[]>absent(), consumer.consume());
    }

    @Test
    public void testLoadExisting() throws Exception {
        bufferQueue = new MappedBufferQueue.Builder(this.file)
                .formatIfNotExists(10000, 1024)
                .headerSyncInterval(1)
                .build();
        BufferQueue.Publisher publisher = bufferQueue.publisher();
        BufferQueue.Consumer consumer = bufferQueue.consumer();

        publisher.publish(MSG1);
        bufferQueue.close();
        bufferQueue = new MappedBufferQueue.Builder(file)
                .formatIfNotExists(1000000, 1024)
                .headerSyncInterval(10)
                .build();
        assertArrayEquals(MSG1, bufferQueue.consumer().consume().get());
    }

    @Test
    public void testPublishMultiConsume() throws Exception {
        bufferQueue = new MappedBufferQueue.Builder(this.file)
                .formatIfNotExists(5096, 102)
                .headerSyncInterval(1)
                .build();
        System.out.println(bufferQueue.maxNumEntries());
        BufferQueue.Publisher publisher = bufferQueue.publisher();
        BufferQueue.Consumer consumer = bufferQueue.consumer();

        Queue<String> msgs = new LinkedList<String>();

        for (int i = 0; i < 100; i++) {
            String m = UUID.randomUUID().toString();
            if (publisher.publish(m.getBytes())) {
                msgs.add(m);
            }
        }
        bufferQueue.printBufferSkeleton();

        int size = msgs.size();
        for (int i = 0; i < size / 2; i++) {
            byte[] m1 = msgs.poll().getBytes();
            byte[] m2 = consumer.consume().get();
            assertArrayEquals(m1, m2);
        }
        System.out.println("Post Consume");
        bufferQueue.printBufferSkeleton();

        for (int i = 0; i < 100; i++) {
            String m = UUID.randomUUID().toString();
            try {
                if (publisher.publish(m.getBytes())) {
                    msgs.add(m);
                }
            }
            catch (IllegalArgumentException e) {
                System.out.println(i);
                throw e;
            }
        }

        size = msgs.size();
        for (int i = 0; i < size; i++) {
            byte[] m = consumer.consume().get();
            assertArrayEquals(msgs.poll().getBytes(), m);
        }

        assertEquals(Optional.<byte[]>absent(), consumer.consume());
    }

    @Test
    public void testSeqOverflowingPublishConsume() throws Exception {
        bufferQueue = new MappedBufferQueue.Builder(this.file)
                .formatIfNotExists(8192, 256)
                .headerSyncInterval(1)
                .build();
        final BufferQueue.Publisher publisher = bufferQueue.publisher();
        final BufferQueue.Consumer consumer = bufferQueue.consumer();

        final int numMessages = 100;
        final List<byte[]> msgs = new ArrayList<byte[]>();

        for (int i = 0; i < 15; i++) {
            assertTrue(publisher.publish(("" + i).getBytes()));
        }
        bufferQueue.printBufferSkeleton("After 1st publish");
        for (int i = 0; i < 15; i++) {
            assertEquals("" + i, new String(consumer.consume().get()));
        }
        bufferQueue.printBufferSkeleton("After 1st consumption");

        for (int i = 0; i < 15; i++) {
            assertTrue(publisher.publish(("" + i).getBytes()));
            bufferQueue.printBufferSkeleton("After second publish (" + i + ")");
        }
        for (int i = 0; i < 15; i++) {
            assertEquals("" + i, new String(consumer.consume().get()));
        }

        for (int i = 0; i < 15; i++) {
            assertTrue(publisher.publish(("" + i).getBytes()));
        }
        for (int i = 0; i < 15; i++) {
            assertEquals("" + i, new String(consumer.consume().get()));
        }
    }

    @Test
    public void testParallelPublishConsume() throws Exception {
        bufferQueue = new MappedBufferQueue.Builder(this.file)
                .formatIfNotExists(8192, 128)
                .headerSyncInterval(1)
                .build();
        final BufferQueue.Publisher publisher = bufferQueue.publisher();
        final BufferQueue.Consumer consumer = bufferQueue.consumer();

        final int numMessages = 1000;
        final List<byte[]> msgs = new ArrayList<byte[]>();
        final Random random = new Random();

        final CountDownLatch countDownLatch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newFixedThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        });

        Runnable producerRunnable = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numMessages; i++) {
                    try {
                        byte[] m = UUID.randomUUID().toString().getBytes();
                        if (publisher.publish(m)) {
                            msgs.add(m);
                            System.out.println("Published: " + i + " m:" + new String(m));
                        }
                        else {
                            i--;
                            Thread.sleep(random.nextInt(100) + 1);
                        }
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println("DONE PUBLISHING");
                countDownLatch.countDown();
            }
        };

        Runnable consumerRunnable = new Runnable() {
            @Override
            public void run() {
                int n = 0;
                while (n < numMessages) {
                    try {
                        Optional<byte[]> optional = consumer.consume();
                        if (optional.isPresent()) {
                            System.out.println("Asserting: " + n
                                    + " msgs.size(): " + msgs.size()
                                    + " m: " + new String(optional.get())
                                    + " assert:" + Arrays.equals(msgs.get(n), optional.get()));
                            n++;
                        }
                        else {
                            System.out.println("Absent: " + n
                                    + " consumeCursor: " + ((MappedBufferQueue) bufferQueue).consumeCursor.get()
                                    + " publishCursor: " + ((MappedBufferQueue) bufferQueue).publishCursor.get()
                            );
                            Thread.sleep(random.nextInt(100) + 1);
                        }
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println("DONE CONSUMING");
                countDownLatch.countDown();
            }
        };

        executorService.submit(producerRunnable);
        executorService.submit(consumerRunnable);

        countDownLatch.await();
        System.out.println("FIN");
    }
}
