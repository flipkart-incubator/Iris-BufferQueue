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
    static final long OFFSET_MAX_MSG_LENGTH = 0;   // 0

    /* The following is offset by Long.SIZE instead of Integer.SIZE in order to ensure that
               the address is memory aligned.
             */
    @VisibleForTesting
    static final long OFFSET_WRITE_CURSOR = OFFSET_MAX_MSG_LENGTH + Long.SIZE; // 8

    @VisibleForTesting
    static final long OFFSET_READ_CURSOR = OFFSET_WRITE_CURSOR + Long.SIZE;   // 16

    private final ByteBuffer headerBuffer;
    private int maxDataLengthCached;

    private final ReadWriteLock writeCursorReadWritelock = new ReentrantReadWriteLock();
    private final Lock writeCursorReadLock = writeCursorReadWritelock.readLock();
    private final Lock writeCursorWriteLock = writeCursorReadWritelock.writeLock();

    private final ReadWriteLock readCursorReadWritelock = new ReentrantReadWriteLock();
    private final Lock readCursorReadLock = readCursorReadWritelock.readLock();
    private final Lock readCursorWriteLock = readCursorReadWritelock.writeLock();

    MappedHeader(ByteBuffer headerBuffer) {
        this.headerBuffer = headerBuffer;
    }

    void format(int maxDataLength) {
        headerBuffer.putInt((int) OFFSET_MAX_MSG_LENGTH, maxDataLength);
        headerBuffer.putLong((int) OFFSET_READ_CURSOR, 1);
        headerBuffer.putLong((int) OFFSET_WRITE_CURSOR, 1);
    }

    public int maxDataLength() {
        return headerBuffer.getInt((int) OFFSET_MAX_MSG_LENGTH);
    }

    long writeCursor() {
        try {
            writeCursorReadLock.lock();
            return headerBuffer.getLong((int) MappedHeader.OFFSET_WRITE_CURSOR);
        }
        finally {
            writeCursorReadLock.unlock();
        }
    }

    long writeCursor(long n) {
        try {
            writeCursorWriteLock.lock();
            long currentValue = writeCursor();
            if (n > currentValue) {
                headerBuffer.putLong((int) MappedHeader.OFFSET_WRITE_CURSOR, n);
                return n;
            }
        }
        finally {
            writeCursorWriteLock.unlock();
        }
        return writeCursor();
    }

    long readCursor() {
        try {
            readCursorReadLock.lock();
            return headerBuffer.getLong((int) MappedHeader.OFFSET_READ_CURSOR);
        }
        finally {
            readCursorReadLock.unlock();
        }
    }

    long readCursor(long n) {
        try {
            readCursorWriteLock.lock();
            long currentValue = readCursor();
            if (n > currentValue) {
                headerBuffer.putLong((int) MappedHeader.OFFSET_READ_CURSOR, n);
                return n;
            }
        }
        finally {
            readCursorWriteLock.unlock();
        }
        return readCursor();
    }
}
