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

package com.flipkart.iris.bufferqueue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import static junit.framework.Assert.*;

public class BufferQueueEntryTest {

    BufferQueueEntry entry;
    long cursor;
    ByteBuffer byteBuffer;
    Random random = new Random();

    @Before
    public void setUp() throws Exception {
        byteBuffer = ByteBuffer.allocate(4096);
        byteBuffer.putLong(BufferQueueEntry.OFFSET_CURSOR, 0);
    }

    @Test
    public void testNewClaim() throws Exception {
        cursor = randomCursor();
        entry = new BufferQueueEntry(byteBuffer, cursor);
        assertEquals(cursor, entry.cursor);
        assertEquals(0, BufferQueueEntry.OFFSET_CURSOR);
        assertFalse(entry.isPublished());
        assertFalse(entry.isConsumed());
    }

    @Ignore
    @Test(expected = IllegalArgumentException.class)
    public void testNewUnclaimed() throws Exception {
        entry = new BufferQueueEntry(byteBuffer);
    }

    @Ignore
    @Test(expected = IllegalArgumentException.class)
    public void testNewUnpublished() throws Exception {
        new BufferQueueEntry(byteBuffer, randomCursor());
        new BufferQueueEntry(byteBuffer);
    }

    @Test
    public void testNewPublishedUnconsumed() throws Exception {
        long cursor = randomCursor();
        new BufferQueueEntry(byteBuffer, cursor).markPublished();
        entry = new BufferQueueEntry(byteBuffer);
        assertEquals(cursor, entry.cursor);
        assertEquals(cursor, entry.readCursor());
        assertTrue(entry.isPublished());
        assertFalse(entry.isConsumed());
    }

    @Ignore
    @Test(expected = IllegalArgumentException.class)
    public void testNewPublishedConsumed() throws Exception {
        long cursor = randomCursor();
        entry = new BufferQueueEntry(byteBuffer, cursor);
        entry.markPublished();
        entry = new BufferQueueEntry(byteBuffer);
        entry.markConsumed();

        new BufferQueueEntry(byteBuffer);
    }

    @Test
    public void testNewClaimPublishedConsumed() throws Exception {
        cursor = randomCursor();
        entry = new BufferQueueEntry(byteBuffer, cursor);
        entry.markPublished();
        entry = new BufferQueueEntry(byteBuffer);
        entry.markConsumed();

        cursor = randomCursor();
        entry = new BufferQueueEntry(byteBuffer, cursor);
        assertEquals(cursor, entry.cursor);
        assertEquals(0, BufferQueueEntry.OFFSET_CURSOR);
        assertFalse(entry.isPublished());
        assertFalse(entry.isConsumed());
    }

    @Test
    public void testSetThenGet() throws Exception {
        cursor = randomCursor();
        entry = new BufferQueueEntry(byteBuffer.duplicate(), cursor);
        String data = UUID.randomUUID().toString();
        entry.set(data.getBytes());
        entry.markPublished();
        assertEquals(data, new String(entry.get()));
    }

    @Test(expected = BufferOverflowException.class)
    public void testSetTooLong() throws Exception {
        cursor = randomCursor();
        ByteBuffer byteBuffer2 = byteBuffer.duplicate();
        byteBuffer2.limit(10);
        entry = new BufferQueueEntry(byteBuffer2, cursor);
        entry.set("Some random string that is longer than 10 bytes".getBytes());
    }

    @Test
    public void testPublished() throws Exception {
        cursor = randomCursor();
        entry = new BufferQueueEntry(byteBuffer.duplicate(), cursor);
        assertFalse(entry.isPublished());
        entry.markPublished();
        assertTrue(entry.isPublished());
    }

    @Test
    public void testLength() throws Exception {
        cursor = randomCursor();
        String data = "hello world!";
        entry = new BufferQueueEntry(byteBuffer.duplicate(), cursor);
        entry.set(data.getBytes());
        entry.markPublished();
        assertEquals(data.length(), entry.dataLength());

        cursor = randomCursor();
        data = "hello there world!";
        entry = new BufferQueueEntry(byteBuffer.duplicate(), cursor);
        entry.set(data.getBytes());
        entry.markPublished();
        assertEquals(data.length(), entry.dataLength());
    }

    @Test
    public void testGetCorruptedBuffer() throws Exception {
        ByteBuffer byteBuffer2 = byteBuffer.duplicate();
        byteBuffer2.putInt(BufferQueueEntry.OFFSET_LENGTH, byteBuffer.limit() * 2);
        entry = new BufferQueueEntry(byteBuffer2, cursor);
        assertEquals("", new String(entry.get()));

        byteBuffer2 = byteBuffer.duplicate();
        byteBuffer2.position(BufferQueueEntry.OFFSET_CHECKSUM);
        byteBuffer2.put("some random bytes".getBytes());
        assertEquals("", new String(entry.get()));

        byteBuffer2 = byteBuffer.duplicate();
        byteBuffer2.position(BufferQueueEntry.OFFSET_DATA);
        byteBuffer2.put("some random bytes".getBytes());
        assertEquals("", new String(entry.get()));
    }

    @Test
    public void testConsumed() throws Exception {
        cursor = randomCursor();
        ByteBuffer byteBuffer2 = byteBuffer.duplicate();
        entry = new BufferQueueEntry(byteBuffer2, cursor);
        entry.markPublished();

        entry = new BufferQueueEntry(byteBuffer2);
        assertFalse(entry.isConsumed());
        entry.markConsumed();
        assertTrue(entry.isConsumed());
    }

    @Test
    public void testCalculateEntryLength() throws Exception {
    }

    long randomCursor() {
        long randCursor;
        do {
            randCursor = random.nextLong();
        } while (randCursor == 0);

        return Math.abs(randCursor);
    }
}
