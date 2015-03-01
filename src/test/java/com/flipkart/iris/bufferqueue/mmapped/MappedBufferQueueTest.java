/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.iris.bufferqueue.mmapped;

import com.flipkart.iris.bufferqueue.BufferQueueEntry;
import com.google.common.base.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.UUID;

import static org.junit.Assert.*;

public class MappedBufferQueueTest {

    File file;
    MappedBufferQueue bufferQueue;
    int numMessages = 10;
    int maxDataLength = 256 * 1024;

    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("bufferqueue-test", ".bq");
        file.delete();

        MappedBufferQueueFactory.format(file, maxDataLength, numMessages);
        bufferQueue = MappedBufferQueueFactory.getInstance(file);
    }

    @After
    public void tearDown() throws Exception {
        file.delete();
    }

    @Test
    public void testSimplePublishConsume() throws Exception {
        byte[] msg = UUID.randomUUID().toString().getBytes();
        bufferQueue.publish(msg);
        BufferQueueEntry bufferQueueEntry = bufferQueue.consume().get();
        assertArrayEquals(msg, bufferQueueEntry.get());
        bufferQueueEntry.markConsumed();
        assertEquals(Optional.<BufferQueueEntry>absent(), bufferQueue.consume());
    }

    @Test
    public void testCircularPublishConsume() throws Exception {

        final ArrayList<byte[]> sampleArray = new ArrayList<byte[]>(numMessages);
        for(int i = 0; i < numMessages; ++i) {
            byte[] msg = UUID.randomUUID().toString().getBytes();
            bufferQueue.publish(msg);
            sampleArray.add(msg);
        }

        for (int i = 0; i < numMessages; ++i) {
            BufferQueueEntry bufferQueueEntry = bufferQueue.consume().get();
            byte[] msg = sampleArray.get(i);
            assertArrayEquals(msg, bufferQueueEntry.get());
            bufferQueueEntry.markConsumed();
        }

        assertEquals(Optional.<BufferQueueEntry>absent(), bufferQueue.consume());

        {
            final byte[] msg = UUID.randomUUID().toString().getBytes();
            bufferQueue.publish(msg);
            final BufferQueueEntry bufferQueueEntry = bufferQueue.consume().get();
            assertArrayEquals(msg, bufferQueueEntry.get());
            bufferQueueEntry.markConsumed();
        }

        {
            final byte[] msg = UUID.randomUUID().toString().getBytes();
            bufferQueue.publish(msg);
            final BufferQueueEntry bufferQueueEntry = bufferQueue.consume().get();
            assertArrayEquals(msg, bufferQueueEntry.get());
            bufferQueueEntry.markConsumed();
        }

    }

    @Test
    public void testFullQueue() throws Exception {
        for(int i = 0; i < numMessages; ++i) {
            final byte[] msg = UUID.randomUUID().toString().getBytes();
            final boolean isPublished = bufferQueue.publish(msg);
            assertTrue("All messages should be published", isPublished);
        }

        assertEquals(numMessages, bufferQueue.size());

        final byte[] msg = UUID.randomUUID().toString().getBytes();
        final boolean isPublished = bufferQueue.publish(msg);
        assertFalse("Should not publish more messages when queue is full", isPublished);
    }
}
