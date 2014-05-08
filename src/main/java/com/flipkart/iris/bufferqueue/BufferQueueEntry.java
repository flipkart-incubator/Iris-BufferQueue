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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents an entry in a <code>BufferQueue</code>. <br/><br/>
 *
 * A <code>BufferQueueEntry</code> is in one of three states:
 *
 * <ol>
 *     <li>Claimed, but not yet published.</li>
 *     <li>Published, but not yet consumed.</li>
 *     <li>Consumed, can be claimed again.</li>
 * </ol>
 *
 * This class is NOT thread-safe, if entry objects are shared
 * across threads, access must be synchronized.
 */
@NotThreadSafe
public class BufferQueueEntry {

    private static final HashFunction checksum = Hashing.crc32();
    private static final int CHECKSUM_BYTES = checksum.bits() / Byte.SIZE;

    @VisibleForTesting static final int OFFSET_CURSOR = 0;
    @VisibleForTesting static final int OFFSET_LENGTH = OFFSET_CURSOR + Long.SIZE;
    @VisibleForTesting static final int OFFSET_CHECKSUM = OFFSET_LENGTH + Integer.SIZE;
    @VisibleForTesting static final int OFFSET_DATA = OFFSET_CHECKSUM + CHECKSUM_BYTES;

    private static final long CURSOR_UNPUBLISHED = 0l;
    private static final long CURSOR_CONSUMED = -1l;

    private final ByteBuffer buf;

    @VisibleForTesting
    long cursor;

    /**
     * To be used when claiming a new entry. The returned entity will be "Claimed, but not yet published". <br/><br/>
     *
     * The argument <code>buf</code> will be used as the backing <code>ByteBuffer</code> for this entry.
     * The argument <code>cursor</code> will be set as the cursor for this entry but it won't be written into
     * <code>buf</code> until the entry is marked as published (after data is written to it). <br/><br/>
     *
     * This constructor is part of the internal API and shouldn't be used by clients.
     * Instead, clients should get instances of <code>BufferQueueEntry</code>s from implementations of
     * {@link com.flipkart.iris.bufferqueue.BufferQueue}.
     *
     * @param buf The <code>ByteBuffer</code> representing an entry.
     * @param cursor The cursor for this entry.
     */
    public BufferQueueEntry(ByteBuffer buf, long cursor) {
        this.buf = buf;
        this.cursor = cursor;
        buf.putLong(OFFSET_CURSOR, CURSOR_UNPUBLISHED);
    }

    /**
     * To be used for entries that are "Published, but not yet consumed". <br/><br/>
     *
     * The argument <code>buf</code> must thus represent such an entry.
     * The entry's <code>cursor</code> will be read from the buf. <br/><br/>
     *
     * This constructor is part of the internal API and shouldn't be used by clients.
     * Instead, clients should get instances of <code>BufferQueueEntry</code>s from implementations of
     * {@link com.flipkart.iris.bufferqueue.BufferQueue}.
     *
     * @param buf The <code>ByteBuffer</code> representing an entry.
     * @throws IllegalStateException If <code>buf</code> does not represent a "Published, but not yet consumed" entry.
     */
    public BufferQueueEntry(ByteBuffer buf) {
        this.buf = buf;
        this.cursor = buf.getLong(OFFSET_CURSOR);

        if (this.cursor <= 0) {
            throw new IllegalArgumentException("The given buffer entry does not represent a \"published, but not yet consumed\" entry");
        }
    }

    /**
     * Set the data of the entry. <b>Important</b>: The entry will not be marked as published after the data is set.
     * This must be done explicitly by calling {@link #markPublished()}. <br/><br/>
     *
     * It is recommended that this method be called in a try block followed by a finally block where the entry is marked
     * as published; the entry must be marked as published even if writing data to the entry failed.
     *
     * @param data The data to set.
     * @throws BufferOverflowException If the length of data is greater than what the entry can hold.
     * @throws java.lang.IllegalStateException If this entry is already published.
     */
    public void set(byte[] data) throws BufferOverflowException {

        if (isPublished()) {
            throw new IllegalStateException("The given buffer entry has already been marked as published and cannot be written to");
        }

        if (calculateEntryLength(data.length) > buf.limit()) {
            throw new BufferOverflowException();
        }

        buf.putInt(OFFSET_LENGTH, data.length);

        buf.position(OFFSET_CHECKSUM);
        buf.put(checksum.hashBytes(data).asBytes());

        buf.position(OFFSET_DATA);
        buf.put(data);

        buf.flip();
    }

    /**
     * Mark this entry as published. <br/><br/>
     *
     * Once an entry is marked as published, it cannot be written to. It is important to mark every entry that is claimed
     * as published because otherwise the entire buffer queue consumption is stalled when it reaches this entry.
     *
     * See {@link #set(byte[])} for best practices around how to call this method.
     */
    public void markPublished() {
        writeCursor(cursor);
    }

    /**
     * Check if the entry is marked as published or not.
     *
     * @return <code>true</code> if the entry is marked as published, <code>false</code> otherwise.
     */
    public boolean isPublished() {
        return readCursor() != CURSOR_UNPUBLISHED;
    }

    /**
     * Get the length of the data in this entry.
     *
     * @return Length of data if this entry is published, -1 otherwise.
     */
    public int dataLength() {
        if (!isPublished()) return -1;

        return buf.getInt(OFFSET_LENGTH);
    }

    /**
     * Get the max length of data that can be written to this entry. <br/><br/>
     *
     * Trying to write more than this amount of data will lead to a {@link java.nio.BufferOverflowException}.
     *
     * @return The max length of data that can be written to this entry.
     */
    public int maxDataLength() {
        return buf.capacity() - OFFSET_DATA;
    }

    /**
     * Get the data published to this entry. <b>Important</b>: The entry will not be marked as consumed after the data
     * read. This must be done explicitly by calling {@link #markConsumed()}. <br/><br/>
     *
     * It is recommended that this method be called in a try block followed by a finally block where the entry is marked
     * as consumed; the entry must be marked as consumed even if processing the data failed.
     *
     * @return The data published to this buffer, empty byte array if nothing is published.
     * @throws java.lang.IllegalStateException If the entry has already been marked as consumed.
     */
    public byte[] get() {

        if (isConsumed()) {
            throw new IllegalStateException("The buffer queue entry has already been marked as consumed, so cannot be read from");
        }

        int length = dataLength();
        if (!isPublished() || length > maxDataLength() || length <= 0) {
            return new byte[0];
        }

        byte[] checksumBytes = new byte[CHECKSUM_BYTES];
        buf.position(OFFSET_CHECKSUM);
        buf.get(checksumBytes);

        byte[] data = new byte[length];
        buf.position(OFFSET_DATA);
        buf.get(data);

        if (!Arrays.equals(checksum.hashBytes(data).asBytes(), checksumBytes)) {
            return new byte[0];
        }

        return data;
    }

    /**
     * Mark this entry as published. <br/><br/>
     *
     * Once an entry is marked as consumed, it cannot be read from. It is important to mark every entry that is obtained
     * as consumed because otherwise the entire buffer queue consumption is stalled.
     *
     * See {@link #get()} for best practices around how to call this method.
     */
    public void markConsumed() {
        writeCursor(CURSOR_CONSUMED);
    }

    /**
     * Check if the entry is marked as consumed or not.
     *
     * @return <code>true</code> if the entry is marked as consumed, <code>false</code> otherwise.
     */
    public boolean isConsumed() {
        return readCursor() == CURSOR_CONSUMED;
    }

    /**
     * Get the cursor value written in the backing buffer.
     *
     * @return The cursor value.
     */
    @VisibleForTesting
    long readCursor() {
        return buf.getLong(OFFSET_CURSOR);
    }

    /**
     * Write the cursor value to the backing buffer.
     *
     * @param cursor The cursor value to write.
     */
    @VisibleForTesting
    void writeCursor(long cursor) {
        buf.putLong(OFFSET_CURSOR, cursor);
    }

    /**
     * Given message data length to be be written, calculate the size of the byte buffer that must be used for
     * creating an entry. <br/><br/>
     *
     * Intended as a helper method to be used by BufferQueue implementations.
     *
     * @param messageDataLength The length of data based on which buffer queue entry length must be computed.
     * @return The computed length of buffer to be used to create the entry.
     */
    public static int calculateEntryLength(int messageDataLength) {
        return OFFSET_DATA + messageDataLength;
    }
}
