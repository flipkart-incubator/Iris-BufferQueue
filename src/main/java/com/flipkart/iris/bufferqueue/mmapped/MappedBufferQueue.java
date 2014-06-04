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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.validation.constraints.NotNull;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A BufferQueue implementation backed by a memory mapped file.
 *
 * @see com.flipkart.iris.bufferqueue.BufferQueue
 */
public class MappedBufferQueue implements BufferQueue {

    /**
     * Maximum block size that can be used.
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

    private final HeaderSyncThread headerSyncThread;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private boolean isClosed;

    private final MappedHeader mappedHeader;
    private final MappedEntries mappedEntries;


    public static class Builder {
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
            return new MappedBufferQueue(this);
        }
    }

    private MappedBufferQueue(Builder builder) throws IOException {
        boolean fileExists = builder.file.exists();

        if (!fileExists && !builder.formatIfNotExists) {
            throw new FileNotFoundException("File doesn't exist and creation not requested");
        }

        long fileSize = fileExists ? builder.file.length() : builder.fileSize;

        ByteBuffer fileBuffer = Helper.mapFile(builder.file, fileSize); // creates file if it doesn't already exist
        randomAccessFile = new RandomAccessFile(builder.file, "rw");
        this.fileChannel = randomAccessFile.getChannel();

        this.mappedHeader = getHeaderBuffer(fileBuffer);
        if (!fileExists) {
            Preconditions.checkArgument(builder.blockSize < MAX_BLOCK_SIZE
                    , "blockSize must be <= %s", MAX_BLOCK_SIZE);
            mappedHeader.format(builder.blockSize);
        }

        this.mappedEntries = getEntriesBuffer(fileBuffer);
        if (!fileExists) {
            mappedEntries.format();
        }

        blockSize = mappedHeader.blockSize();
        consumeCursor.set(mappedHeader.readConsumeCursor());
        publishCursor.set(mappedHeader.readPublishCursor());

        headerSyncThread = new HeaderSyncThread(builder.headerSyncInterval);
        headerSyncThread.start();
    }

    private MappedHeader getHeaderBuffer(ByteBuffer fileBuffer) {
        ByteBuffer headerBuffer = subBuffer(fileBuffer, 0, MappedHeader.HEADER_LENGTH);
        return new MappedHeader(headerBuffer);
    }

    private MappedEntries getEntriesBuffer(ByteBuffer fileBuffer) {
        ByteBuffer entriesBuffer = subBuffer(fileBuffer, MappedHeader.HEADER_LENGTH);
        return new MappedEntries(entriesBuffer);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            headerSyncThread.disable();
            syncHeader();
            randomAccessFile.close();
            fileChannel.close();
            isClosed = true;
        }
    }

    private void checkNotClosed() {
        if (isClosed) {
            throw new ClosedBufferQueueException();
        }
    }

    public long forwardConsumeCursor() {
        long consumeCursorVal;
        while ((consumeCursorVal = consumeCursor.get()) < publishCursor.get()) {
            MappedBufferQueueEntry entry = mappedEntries.getEntry(consumeCursorVal);
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
     * Similar care needs to be taken when using this method as that which is required when using {@link #claim()}. See
     * the docs for that method to understand these care instructions.
     *
     * @see #claim()
     */
    public Optional<MappedBufferQueueEntry> claim(byte numBlocks) {
        checkNotClosed();
        long n;
        do {
            n = publishCursor.get();
            if (n - consumeCursor.get() >= maxNumEntries() - numBlocks) {
                // TODO: decide if we really do want to do this
                forwardConsumeCursor();
                if (n - consumeCursor.get() >= maxNumEntries() - numBlocks) {
                    return Optional.absent();
                }
            }
        }
        while (!publishCursor.compareAndSet(n, n + numBlocks));

        return Optional.of(mappedEntries.makeEntry(n, numBlocks));
    }

    @Override
    public Optional<MappedBufferQueueEntry> claim() {
        return claim((byte) 1);
    }

    @Override
    public Optional<MappedBufferQueueEntry> claimFor(int dataSize) {
        if (dataSize > maxDataLength()) {
            throw new IllegalArgumentException("Cannot create buffer for requested data size in this BufferQueue");
        }

        int dataPlusMetadataSize = dataSize + metadataOverhead();
        byte numBlocks = (byte) (dataPlusMetadataSize / blockSize
                        + dataPlusMetadataSize % blockSize != 0 ? 1 : 0);
        return claim(numBlocks);
    }

    @Override
    public boolean publish(byte[] data) throws BufferOverflowException {
        Optional<MappedBufferQueueEntry> entry = claimFor(data.length);
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
    public Optional<MappedBufferQueueEntry> peek() {
        checkNotClosed();

        long readCursorVal = forwardConsumeCursor();
        if (readCursorVal < publishCursor.get()) {
            return Optional.of(mappedEntries.getEntry(readCursorVal));
        }
        return Optional.absent();
    }

    @Override
    public List<MappedBufferQueueEntry> peek(int n) {
        checkNotClosed();
        List<MappedBufferQueueEntry> bufferQueueEntries = Lists.newArrayList();

        long readCursorVal = forwardConsumeCursor();
        for (int i = 0; i < Math.min(n, publishCursor.get() - readCursorVal); i++) {
            MappedBufferQueueEntry entry = mappedEntries.getEntry(readCursorVal + i);
            if (!entry.isPublished()) break;
            bufferQueueEntries.add(entry);
        }

        return bufferQueueEntries;
    }

    @Override
    public Optional<byte[]> consume() {
        Optional<MappedBufferQueueEntry> entry = peek();
        try {
            if (entry.isPresent()) {
                return Optional.of(entry.get().get());
            }
            return Optional.absent();
        }
        finally {
            if (entry.isPresent()) {
                entry.get().markConsumed();
            }
        }
    }

    @Override
    public List<byte[]> consume(int n) {
        List<MappedBufferQueueEntry> entries = peek(n);
        List<byte[]> dataList = Lists.newArrayListWithCapacity(entries.size());
        for (MappedBufferQueueEntry entry : entries) {
            try {
                dataList.add(entry.get());
            }
            finally {
                entry.markConsumed();
            }
        }
        return dataList;
    }

    public int metadataOverhead() {
        return MappedBufferQueueEntry.OFFSET_ENTRY + BufferQueueEntry.metadataOverhead();
    }

    @Override
    public int maxDataLength() {
        return Byte.MAX_VALUE * blockSize - metadataOverhead();
    }

    @Override
    public long maxNumEntries() {
        return mappedEntries.capacity;
    }

    @Override
    public long size() {
        return (publishCursor.get() - consumeCursor.get());
    }

    @Override
    public boolean isFull() {
        return size() == maxNumEntries();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    static class MappedHeader {

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

    private synchronized void syncHeader() {
        if (fileChannel == null || !fileChannel.isOpen()) {
            return;
        }

        try {
            FileLock lock = fileChannel.lock();
            if (lock != null) {
                // lock maybe null due to a bug in Java 1.6: http://bugs.java.com/view_bug.do?bug_id=7024131
                try {
                    long currentWriteCursor = publishCursor.get();
                    long persistedWriteCursor = mappedHeader.commitPublishCursor(currentWriteCursor);
                    publishCursor.compareAndSet(currentWriteCursor, persistedWriteCursor);

                    long currentReadCursor = consumeCursor.get();
                    long persistedReadCursor = mappedHeader.commitConsumeCursor(currentReadCursor);
                    consumeCursor.compareAndSet(currentReadCursor, persistedReadCursor);
                }
                finally {
                    if (lock.isValid()) {
                        lock.release();
                    }
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
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
                        syncHeader();
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

    public class MappedEntries {

        @VisibleForTesting static final int OFFSET_NUM_BLOCKS = 0;
        @VisibleForTesting static final int OFFSET_ENTRY = OFFSET_NUM_BLOCKS + 1;

        private final ByteBuffer entriesBuffer;
        public final int blockSize;
        public final int capacity;

        MappedEntries(ByteBuffer entriesBuffer) {
            this.entriesBuffer = entriesBuffer;
            this.blockSize = mappedHeader.blockSize();
            this.capacity = entriesBuffer.limit() / blockSize;
        }

        public void format() {
            // TODO: think about whether we really need the following block
            long numMessages = capacity;
            for (int i = 0; i < numMessages; i++) {
                makeEntry(i).set(new byte[0]);
            }
        }

        @VisibleForTesting
        @NotNull
        MappedBufferQueueEntry makeEntry(long cursor, byte numBlocks) {
            int offset = (int) ((cursor % capacity) * blockSize);
            // TODO: check end of maxNumEntries
            ByteBuffer buf = subBuffer(entriesBuffer, offset);
            buf.put(OFFSET_NUM_BLOCKS, numBlocks);
            return new MappedBufferQueueEntry(subBuffer(buf, OFFSET_ENTRY), cursor);
        }

        @VisibleForTesting
        @NotNull
        MappedBufferQueueEntry makeEntry(long cursor) {
            return makeEntry(cursor, (byte) 1);
        }

        @VisibleForTesting
        @NotNull
        MappedBufferQueueEntry getEntry(long cursor) {
            int offset = (int) ((cursor % capacity) * blockSize);
            ByteBuffer buf = subBuffer(entriesBuffer, offset);
            int numBlocks = buf.get(OFFSET_NUM_BLOCKS);
            return new MappedBufferQueueEntry(subBuffer(buf, OFFSET_ENTRY));
        }
    }

    public class MappedBufferQueueEntry extends BufferQueueEntry {

        @VisibleForTesting static final int OFFSET_CURSOR = 0;
        @VisibleForTesting static final int OFFSET_ENTRY = OFFSET_CURSOR + Long.SIZE;

        private static final long CURSOR_UNPUBLISHED = -1l;
        private static final long CURSOR_CONSUMED = -2l;

        private final ByteBuffer buf;
        private final long cursor;

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
        MappedBufferQueueEntry(ByteBuffer buf, long cursor) {
            super(subBuffer(buf, OFFSET_ENTRY));
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
        MappedBufferQueueEntry(ByteBuffer buf) {
            super(subBuffer(buf, OFFSET_ENTRY));
            this.buf = buf;
            this.cursor = buf.getLong(OFFSET_CURSOR);
        }

        @Override
        public void markPublished() {
            writeCursor(cursor);
            forwardConsumeCursor();
        }

        @Override
        public boolean isPublished() {
            return readCursor() != CURSOR_UNPUBLISHED;
        }

        @Override
        public void markConsumed() {
            writeCursor(CURSOR_CONSUMED);
            forwardConsumeCursor();
        }

        @Override
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
    }

    /**
     * Like {@link String#substring(int, int)}, but for ByteBuffer.
     *
     * @param buf The <code>ByteBuffer</code> to slice
     * @param start The start position of the new ByteBuffer
     * @param length The length of the new ByteBuffer
     * @return A new ByteBuffer of specified length which starts at specified position of buf
     */
    private static ByteBuffer subBuffer(ByteBuffer buf, int start, int length) {
        buf = buf.duplicate();
        buf.position(start);
        buf = buf.slice();
        buf.limit(length);
        buf.rewind();
        return buf;
    }

    /**
     * Like {@link String#substring(int)}, but for ByteBuffer.
     *
     * @param buf The <code>ByteBuffer</code> to slice
     * @param start The start position of the new ByteBuffer
     * @return A new ByteBuffer which starts at specified position of buf
     */
    private static ByteBuffer subBuffer(ByteBuffer buf, int start) {
        buf = buf.duplicate();
        buf.position(start);
        buf = buf.slice();
        buf.rewind();
        return buf;
    }
}
