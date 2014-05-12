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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.flipkart.iris.bufferqueue.mmapped.MappedHeader.HEADER_LENGTH;

/**
 * A BufferQueue implementation backed by a memory mapped file.
 *
 * @see com.flipkart.iris.bufferqueue.BufferQueue
 */
public class MappedBufferQueue implements BufferQueue {

    /**
     * Maximum block size that can be used.
     *
     * @see #format(File, int, int)
     */
    private static final int MAX_BLOCK_SIZE = 1024 * 1024; // 1mb

    /**
     * Number of milliseconds to wait between syncing the cursors to
     * the header. If the application crashes, messages corresponding to
     * the un-synced cursors may be lost.
     *
     * @see MappedBufferQueue.HeaderSyncThread
     */
    public static final int DEFAULT_SYNC_INTERVAL = 10; // milliseconds

    private final Integer blockSize;
    private final AtomicLong consumeCursor = new AtomicLong(0);
    private final AtomicLong publishCursor = new AtomicLong(0);

    private final FileChannel fileChannel;

    private final MappedHeader mappedHeader;
    private final MappedEntries mappedEntries;

    private static void format(File file, int fileSize, int blockSize) throws IOException {
        Preconditions.checkArgument(blockSize < MAX_BLOCK_SIZE
                , "blockSize must be <= %s", MAX_BLOCK_SIZE);

        Helper.createFile(file, fileSize);
        ByteBuffer fileBuffer = Helper.mapFile(file, blockSize);

        MappedHeader headerBuffer = getHeaderBuffer(fileBuffer);
        headerBuffer.format(blockSize);

        MappedEntries entriesBuffer = getEntriesBuffer(fileBuffer, headerBuffer);
        entriesBuffer.format();
    }

    private MappedBufferQueue(File file, int headerSyncInterval) throws IOException {

        ByteBuffer fileBuffer = Helper.mapFile(file, file.length());
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();

        this.mappedHeader = getHeaderBuffer(fileBuffer);
        this.mappedEntries = getEntriesBuffer(fileBuffer, mappedHeader);

        blockSize = mappedHeader.blockSize();
        consumeCursor.set(mappedHeader.readConsumeCursor());
        publishCursor.set(mappedHeader.readPublishCursor());

        HeaderSyncThread headerSyncThread = new HeaderSyncThread(headerSyncInterval);
        headerSyncThread.start();
    }

    private static MappedHeader getHeaderBuffer(ByteBuffer fileBuffer) {
        ByteBuffer headerBuffer = fileBuffer.duplicate();
        headerBuffer.limit(HEADER_LENGTH);
        headerBuffer.rewind();
        return new MappedHeader(headerBuffer);
    }

    private static MappedEntries getEntriesBuffer(ByteBuffer fileBuffer, MappedHeader mappedHeader) {
        fileBuffer.position(HEADER_LENGTH);
        ByteBuffer entriesBuffer = fileBuffer.slice();
        entriesBuffer.rewind();
        return new MappedEntries(entriesBuffer, mappedHeader);
    }

    public long forwardConsumeCursor() {
        long consumeCursorVal;
        while ((consumeCursorVal = consumeCursor.get()) < publishCursor.get()) {
            BufferQueueEntry entry = mappedEntries.getEntry(consumeCursorVal);
            if (!entry.isPublished() || !entry.isConsumed()) {
                break;
            }
            consumeCursor.compareAndSet(consumeCursorVal, consumeCursorVal + 1);
        }
        return consumeCursorVal;
    }

    /**
     * Claim the next entry in the buffer queue, ensuring that it spans across the given number of blocks. <br/><br/>
     *
     * Similar care needs to be taken when using this method as that which is required when using {@link #next()}. See
     * the docs for that method to understand these care instructions.
     *
     * @see #next()
     */
    public Optional<BufferQueueEntry> next(int numBlocks) {
        long n;
        do {
            n = publishCursor.get();
            if (n - consumeCursor.get() >= capacity() - numBlocks) {
                forwardConsumeCursor();
                if (n - consumeCursor.get() >= capacity() - numBlocks) {
                    return Optional.absent();
                }
            }
        }
        while (!publishCursor.compareAndSet(n, n + numBlocks));

        return Optional.of(mappedEntries.makeEntry(n, numBlocks));
    }

    @Override
    public Optional<BufferQueueEntry> next() {
        return next(1);
    }

    @Override
    public Optional<BufferQueueEntry> nextFor(int dataSize) {
        // TODO: account for internal entry overhead
        return next(dataSize / blockSize + dataSize % blockSize != 0 ? 1 : 0);
    }

    @Override
    public boolean publish(byte[] data) throws BufferOverflowException {
        Optional<BufferQueueEntry> entry = nextFor(data.length);
        if (!entry.isPresent()) return false;

        try {
            entry.get().set(data);
        }
        finally {
            entry.get().markPublished();
        }
        return true;
    }

    @Override
    public Optional<BufferQueueEntry> consume() {
        long readCursorVal = forwardConsumeCursor();
        if (readCursorVal < publishCursor.get()) {
            return Optional.of(mappedEntries.getEntry(readCursorVal));
        }
        return Optional.absent();
    }

    @Override
    public List<BufferQueueEntry> consume(int n) {
        List<BufferQueueEntry> bufferQueueEntries = Lists.newArrayList();

        long readCursorVal = forwardConsumeCursor();
        for (int i = 0; i < Math.min(n, publishCursor.get() - readCursorVal); i++) {
            BufferQueueEntry entry = mappedEntries.getEntry(readCursorVal + i);
            if (!entry.isPublished()) break;
            bufferQueueEntries.add(entry);
        }

        return bufferQueueEntries;
    }

    @Override
    public int maxDataLength() {
        // TODO: should subtract internal overhead
        return Byte.MAX_VALUE * blockSize;
    }

    @Override
    public long capacity() {
        return mappedEntries.capacity / blockSize;
    }

    @Override
    public long size() {
        return (publishCursor.get() - consumeCursor.get());
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
                                long currentWriteCursor = publishCursor.get();
                                long persistedWriteCursor = mappedHeader.commitPublishCursor(currentWriteCursor);
                                publishCursor.compareAndSet(currentWriteCursor, persistedWriteCursor);

                                long currentReadCursor = consumeCursor.get();
                                long persistedReadCursor = mappedHeader.commitConsumeCursor(currentReadCursor);
                                consumeCursor.compareAndSet(currentReadCursor, persistedReadCursor);
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

    public class Builder {
        private File file;
        private int headerSyncInterval = DEFAULT_SYNC_INTERVAL;

        private boolean formatIfNotExists = false;
        private int blockSize;
        private int fileSize;

        public Builder(File file) {
            this.file = file;
        }

        public Builder headerSyncInterval(int headerSyncInterval) {
            this.headerSyncInterval = headerSyncInterval;
            return this;
        }

        public Builder formatIfNotExists(int fileSize, int blockSize) {
            formatIfNotExists = true;
            this.blockSize = blockSize;
            this.fileSize = fileSize;
            return this;
        }

        public MappedBufferQueue build() throws IOException {
            if (!file.exists() && formatIfNotExists) {
                format(file, fileSize, blockSize);
            }

            return new MappedBufferQueue(file, headerSyncInterval);
        }
    }
}
