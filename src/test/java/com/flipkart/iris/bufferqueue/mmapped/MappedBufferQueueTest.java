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
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MappedBufferQueueTest {

    File file;
    MappedBufferQueue bufferQueue;

    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("bufferqueue-test", ".bq");
        file.delete();
        int maxDataLength = 256 * 1024;
        long numMessages = 1000;
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
}
