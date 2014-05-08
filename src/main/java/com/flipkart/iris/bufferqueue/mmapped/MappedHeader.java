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

import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class MappedHeader {

    @VisibleForTesting
    static final int HEADER_LENGTH = 4096;

    @VisibleForTesting
    static final long OFFSET_BLOCK_SIZE = 0;   // 0

    /* The following is offset by Long.SIZE instead of Integer.SIZE in order to ensure that
               the address is memory aligned.
             */
    @VisibleForTesting
    static final long OFFSET_PUBLISH_CURSOR = OFFSET_BLOCK_SIZE + Long.SIZE; // 8

    @VisibleForTesting
    static final long OFFSET_CONSUME_CURSOR = OFFSET_PUBLISH_CURSOR + Long.SIZE;   // 16

    private final ByteBuffer headerBuffer;
    private int maxDataLengthCached;

    private final ReadWriteLock publishCursorReadWritelock = new ReentrantReadWriteLock();
    private final Lock publishCursorReadLock = publishCursorReadWritelock.readLock();
    private final Lock publishCursorWriteLock = publishCursorReadWritelock.writeLock();

    private final ReadWriteLock consumeCursorReadWritelock = new ReentrantReadWriteLock();
    private final Lock consumeCursorReadLock = consumeCursorReadWritelock.readLock();
    private final Lock consumeCursorWriteLock = consumeCursorReadWritelock.writeLock();

    MappedHeader(ByteBuffer headerBuffer) {
        this.headerBuffer = headerBuffer;
    }

    void format(int maxDataLength) {
        headerBuffer.putInt((int) OFFSET_BLOCK_SIZE, maxDataLength);
        headerBuffer.putLong((int) OFFSET_CONSUME_CURSOR, 0);
        headerBuffer.putLong((int) OFFSET_PUBLISH_CURSOR, 0);
    }

    public int blockSize() {
        return headerBuffer.getInt((int) OFFSET_BLOCK_SIZE);
    }

    long readPublishCursor() {
        try {
            publishCursorReadLock.lock();
            return headerBuffer.getLong((int) MappedHeader.OFFSET_PUBLISH_CURSOR);
        }
        finally {
            publishCursorReadLock.unlock();
        }
    }

    long commitPublishCursor(long n) {
        try {
            publishCursorWriteLock.lock();
            long currentValue = readPublishCursor();
            if (n > currentValue) {
                headerBuffer.putLong((int) MappedHeader.OFFSET_PUBLISH_CURSOR, n);
                return n;
            }
        }
        finally {
            publishCursorWriteLock.unlock();
        }
        return readPublishCursor();
    }

    long readConsumeCursor() {
        try {
            consumeCursorReadLock.lock();
            return headerBuffer.getLong((int) MappedHeader.OFFSET_CONSUME_CURSOR);
        }
        finally {
            consumeCursorReadLock.unlock();
        }
    }

    long commitConsumeCursor(long n) {
        try {
            consumeCursorWriteLock.lock();
            long currentValue = readConsumeCursor();
            if (n > currentValue) {
                headerBuffer.putLong((int) MappedHeader.OFFSET_CONSUME_CURSOR, n);
                return n;
            }
        }
        finally {
            consumeCursorWriteLock.unlock();
        }
        return readConsumeCursor();
    }
}
