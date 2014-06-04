package com.flipkart.iris.bufferqueue.mmapped;

import com.flipkart.iris.bufferqueue.BufferQueue;
import com.google.common.base.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MappedBufferQueueTest {
    private static final byte[] MSG1 = "hello world".getBytes();

    File dir;
    BufferQueue bufferQueue;

    @Before
    public void setUp() throws Exception {
        File file = File.createTempFile("bufferqueue-test", Long.toString(System.nanoTime()));
        file.delete();
        dir = file;

        bufferQueue = new MappedBufferQueue.Builder(dir)
                .formatIfNotExists(1000000, 1024)
                .headerSyncInterval(10)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        bufferQueue.close();
        if (!dir.delete()) {
            System.err.println("Couldn't delete temporary directory that was created: " + dir);
        }
    }

    @Test
    public void testSimplePublishConsume() throws Exception {
        bufferQueue.publish(MSG1);
        assertArrayEquals(MSG1, bufferQueue.consume().get());
        assertEquals(Optional.<byte[]>absent(), bufferQueue.consume());
    }

    @Test
    public void testLoadExisting() throws Exception {
        bufferQueue.publish(MSG1);
        bufferQueue.close();
        bufferQueue = new MappedBufferQueue.Builder(dir)
                .formatIfNotExists(1000000, 1024)
                .headerSyncInterval(10)
                .build();
        assertArrayEquals(MSG1, bufferQueue.consume().get());
    }
}
