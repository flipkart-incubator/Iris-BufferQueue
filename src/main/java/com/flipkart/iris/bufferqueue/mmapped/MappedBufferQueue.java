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

import com.flipkart.iris.bufferqueue.BufferQueue;
import com.flipkart.iris.bufferqueue.BufferQueueEntry;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A BufferQueue implementation using a memory mapped file.
 *
 * @see com.flipkart.iris.bufferqueue.BufferQueue
 */
public class MappedBufferQueue implements BufferQueue {

    /**
     * Maximum value of the max data length that can be set.
     *
     * @see #format(java.nio.ByteBuffer, int)
     */
    public static final int MAX_MAX_DATA_LENGTH = 1024 * 1024; // 1mb

    /**
     * Number of milliseconds to wait between syncing the cursors to
     * the header. If the application crashes, messages corresponding to
     * the unsynced cursors may be lost.
     *
     * @see MappedBufferQueue.HeaderSyncThread
     */
    public static final long SYNC_INTERVAL = 1000; // milliseconds

    private final Integer maxDataLength;
    private final AtomicLong readCursor = new AtomicLong(1);
    private final AtomicLong writeCursor = new AtomicLong(1);

    private final File file;
    private final ByteBuffer fileBuffer;

    private final FileChannel fileChannel;

    private final MappedHeader mappedHeader;
    private final MappedEntries mappedEntries;

    private final HeaderSyncThread headerSyncThread;

    /**
     * Computes the file size to be used given the max data dataLength
     * and number of messages.
     *
     * @param maxDataLength The max dataLength of data that can be published to the BufferQueue
     * @param numMessages The maximum number unconsumed messages that the buffer will hold
     * @return The computer file size
     */
    static long fileSize(int maxDataLength, long numMessages) {
        return MappedHeader.HEADER_LENGTH
                + BufferQueueEntry.calculateEntryLength(maxDataLength) * numMessages;
    }

    MappedBufferQueue(File file, ByteBuffer fileBuffer) throws FileNotFoundException, IOException {

        fileBuffer = fileBuffer.duplicate();

        this.file = file;
        this.fileBuffer = fileBuffer;

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        this.fileChannel = raf.getChannel();

        this.mappedHeader = getHeaderBuffer(fileBuffer);
        this.mappedEntries = getEntriesBuffer(fileBuffer, mappedHeader);

        maxDataLength = mappedHeader.maxDataLength();
        readCursor.set(mappedHeader.readCursor());
        writeCursor.set(mappedHeader.writeCursor());

        headerSyncThread = new HeaderSyncThread(SYNC_INTERVAL);
        headerSyncThread.start();
    }

    private static MappedHeader getHeaderBuffer(ByteBuffer fileBuffer) {
        ByteBuffer headerBuffer = fileBuffer.duplicate();
        headerBuffer.limit(MappedHeader.HEADER_LENGTH);
        headerBuffer.rewind();
        return new MappedHeader(headerBuffer);
    }

    private static MappedEntries getEntriesBuffer(ByteBuffer fileBuffer, MappedHeader mappedHeader) {
        fileBuffer.position(MappedHeader.HEADER_LENGTH);
        ByteBuffer entriesBuffer = fileBuffer.slice();
        entriesBuffer.rewind();
        return new MappedEntries(entriesBuffer, mappedHeader);
    }

    static void format(ByteBuffer fileBuffer, int maxDataLength) {
        Preconditions.checkArgument(maxDataLength < MAX_MAX_DATA_LENGTH
                , "maxDataLength must be <= %s", MAX_MAX_DATA_LENGTH);

        MappedHeader headerBuffer = getHeaderBuffer(fileBuffer);
        headerBuffer.format(maxDataLength);

        MappedEntries entriesBuffer = getEntriesBuffer(fileBuffer, headerBuffer);
        entriesBuffer.format();
    }

    @Override
    public int maxDataLength() {
        return maxDataLength;
    }

    @Override
    public Optional<BufferQueueEntry> next() {
        if (writeCursor.get() - readCursor.get() >= capacity()) {
            forwardReadCursor();
            if (writeCursor.get() - readCursor.get() >= capacity()) {
                return Optional.absent();
            }
        }

        long n = writeCursor.getAndIncrement();
        return Optional.of(mappedEntries.makeEntry(n));
    }

    @Override
    public boolean publish(byte[] data) throws BufferOverflowException {
        Optional<BufferQueueEntry> entry = next();
        if (!entry.isPresent()) return false;

        try {
            entry.get().set(data);
        }
        finally {
            entry.get().markPublished();
        }
        return true;
    }

    public long forwardReadCursor() {
        long readCursorVal;
        while ((readCursorVal = readCursor.get()) < writeCursor.get()) {
            if (readCursorVal > 0) {
                BufferQueueEntry entry = mappedEntries.getEntry(readCursorVal);
                if (!entry.isPublished() || !entry.isConsumed()) {
                    break;
                }
            }
            readCursor.compareAndSet(readCursorVal, readCursorVal + 1);
        }
        return readCursorVal;
    }

    @Override
    public Optional<BufferQueueEntry> consume() {
        long readCursorVal = forwardReadCursor();
        if (readCursorVal < writeCursor.get()) {
            BufferQueueEntry entry = mappedEntries.getEntry(readCursorVal);
            if (entry.isPublished()) {
                return Optional.of(entry);
            }
        }
        return Optional.absent();
    }

    @Override
    public List<BufferQueueEntry> consume(int n) {
        List<BufferQueueEntry> bufferQueueEntries = Lists.newArrayList();

        long readCursorVal = forwardReadCursor();
        for (int i = 0; i < Math.min(n, writeCursor.get() - readCursorVal); i++) {
            BufferQueueEntry entry = mappedEntries.getEntry(readCursorVal + i);
            if (!entry.isPublished()) break;
            bufferQueueEntries.add(entry);
        }

        return bufferQueueEntries;
    }

    @Override
    public long capacity() {
        return mappedEntries.capacity;
    }

    @Override
    public long size() {
        return (writeCursor.get() - readCursor.get());
    }

    @Override
    public boolean isFull() {
        return size() == capacity();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    private class HeaderSyncThread extends Thread {
        private final long waitMillies;
        private volatile boolean isEnabled = true;

        private HeaderSyncThread(long waitMillies) {
            this.waitMillies = waitMillies;
            this.setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                synchronized (mappedHeader) {
                    if (isEnabled) {
                        try {
                            FileLock lock = fileChannel.lock();
                            try {
                                long currentWriteCursor = writeCursor.get();
                                long persistedWriteCursor = mappedHeader.writeCursor(currentWriteCursor);
                                writeCursor.compareAndSet(currentWriteCursor, persistedWriteCursor);

                                long currentReadCursor = readCursor.get();
                                long persistedReadCursor = mappedHeader.readCursor(currentReadCursor);
                                readCursor.compareAndSet(currentReadCursor, persistedReadCursor);
                            }
                            finally {
                                lock.release();
                            }
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    try {
                        mappedHeader.wait(waitMillies);
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }

        public void disable() {
            isEnabled = false;
        }

        public void enable() {
            isEnabled = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }
}
