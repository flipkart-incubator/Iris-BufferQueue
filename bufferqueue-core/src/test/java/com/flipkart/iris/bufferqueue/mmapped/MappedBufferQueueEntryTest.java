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

//package com.flipkart.iris.bufferqueue.mmapped;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import java.nio.BufferOverflowException;
//import java.nio.ByteBuffer;
//import java.util.Random;
//import java.util.UUID;
//
//import static junit.framework.Assert.*;
//
//public class MappedBufferQueueEntryTest {
//
//    MappedBufferQueue.MappedBufferQueueEntry entry;
//    long cursor;
//    ByteBuffer byteBuffer;
//    Random random = new Random();
//
//    @Before
//    public void setUp() throws Exception {
//        byteBuffer = ByteBuffer.allocate(4096);
//        byteBuffer.putLong(MappedBufferQueue.MappedBufferQueueEntry.OFFSET_CURSOR, 0);
//    }
//
//    @Test
//    public void testNewClaim() throws Exception {
//        cursor = randomCursor();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer, cursor);
//        assertEquals(cursor, entry.cursor);
//        assertEquals(0, MappedBufferQueue.MappedBufferQueueEntry.OFFSET_CURSOR);
//        assertFalse(entry.isPublishedUnconsumed());
//        assertFalse(entry.isConsumed());
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testNewUnclaimed() throws Exception {
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer);
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testNewUnpublished() throws Exception {
//        new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer, randomCursor());
//        new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer);
//    }
//
//    @Test
//    public void testNewPublishedUnconsumed() throws Exception {
//        long cursor = randomCursor();
//        new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer, cursor).markPublishedUnconsumed();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer);
//        assertEquals(cursor, entry.cursor);
//        assertEquals(cursor, entry.readCursor());
//        assertTrue(entry.isPublishedUnconsumed());
//        assertFalse(entry.isConsumed());
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testNewPublishedConsumed() throws Exception {
//        long cursor = randomCursor();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer, cursor);
//        entry.markPublishedUnconsumed();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer);
//        entry.markConsumed();
//
//        new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer);
//    }
//
//    @Test
//    public void testNewClaimPublishedConsumed() throws Exception {
//        cursor = randomCursor();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer, cursor);
//        entry.markPublishedUnconsumed();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer);
//        entry.markConsumed();
//
//        cursor = randomCursor();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer, cursor);
//        assertEquals(cursor, entry.cursor);
//        assertEquals(0, MappedBufferQueue.MappedBufferQueueEntry.OFFSET_CURSOR);
//        assertFalse(entry.isPublishedUnconsumed());
//        assertFalse(entry.isConsumed());
//    }
//
//    @Test
//    public void testSetThenGet() throws Exception {
//        cursor = randomCursor();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer.duplicate(), cursor);
//        String data = UUID.randomUUID().toString();
//        entry.set(data.getBytes());
//        entry.markPublishedUnconsumed();
//        assertEquals(data, new String(entry.get()));
//    }
//
//    @Test(expected = BufferOverflowException.class)
//    public void testSetTooLong() throws Exception {
//        cursor = randomCursor();
//        ByteBuffer byteBuffer2 = byteBuffer.duplicate();
//        byteBuffer2.limit(10);
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer2, cursor);
//        entry.set("Some random string that is longer than 10 bytes".getBytes());
//    }
//
//    @Test
//    public void testPublished() throws Exception {
//        cursor = randomCursor();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer.duplicate(), cursor);
//        assertFalse(entry.isPublishedUnconsumed());
//        entry.markPublishedUnconsumed();
//        assertTrue(entry.isPublishedUnconsumed());
//    }
//
//    @Test
//    public void testLength() throws Exception {
//        cursor = randomCursor();
//        String data = "hello world!";
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer.duplicate(), cursor);
//        entry.set(data.getBytes());
//        entry.markPublishedUnconsumed();
//        assertEquals(data.length(), entry.dataLength());
//
//        cursor = randomCursor();
//        data = "hello there world!";
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer.duplicate(), cursor);
//        entry.set(data.getBytes());
//        entry.markPublishedUnconsumed();
//        assertEquals(data.length(), entry.dataLength());
//    }
//
//    @Test
//    public void testGetCorruptedBuffer() throws Exception {
//        ByteBuffer byteBuffer2 = byteBuffer.duplicate();
//        byteBuffer2.putInt(MappedBufferQueueEntry.OFFSET_LENGTH, byteBuffer.limit() * 2);
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer2, cursor);
//        assertEquals("", new String(entry.get()));
//
//        byteBuffer2 = byteBuffer.duplicate();
//        byteBuffer2.position(MappedBufferQueueEntry.OFFSET_CHECKSUM);
//        byteBuffer2.put("some random bytes".getBytes());
//        assertEquals("", new String(entry.get()));
//
//        byteBuffer2 = byteBuffer.duplicate();
//        byteBuffer2.position(MappedBufferQueueEntry.OFFSET_DATA);
//        byteBuffer2.put("some random bytes".getBytes());
//        assertEquals("", new String(entry.get()));
//    }
//
//    @Test
//    public void testConsumed() throws Exception {
//        cursor = randomCursor();
//        ByteBuffer byteBuffer2 = byteBuffer.duplicate();
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer2, cursor);
//        entry.markPublishedUnconsumed();
//
//        entry = new MappedBufferQueue.MappedBufferQueueEntry(byteBuffer2);
//        assertFalse(entry.isConsumed());
//        entry.markConsumed();
//        assertTrue(entry.isConsumed());
//    }
//
//    @Test
//    public void testCalculateEntryLength() throws Exception {
//    }
//
//    long randomCursor() {
//        long randCursor;
//        do {
//            randCursor = random.nextLong();
//        } while (randCursor == 0);
//
//        return Math.abs(randCursor);
//    }
//}
