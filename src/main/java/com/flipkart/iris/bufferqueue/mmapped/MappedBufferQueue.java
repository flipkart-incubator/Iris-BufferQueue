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

import javax.swing.tree.RowMapper;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
     */
    public static final int DEFAULT_SYNC_INTERVAL = 10; // milliseconds

    private final Integer blockSize;
    public final AtomicLong consumeCursor = new AtomicLong(0);
    public final AtomicLong publishCursor = new AtomicLong(0);

    private final File file;
    private final int headerSyncInterval;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private boolean isClosed;

    @VisibleForTesting final MappedHeader mappedHeader;
    private final MappedEntries mappedEntries;
    private Publisher publisher;
    private Consumer consumer;


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
            Preconditions.checkArgument(blockSize > metadataOverhead(), "blockSize must be greater than " + metadataOverhead());

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

        this.file = builder.file;
        ByteBuffer fileBuffer = Helper.mapFile(file, fileSize); // creates file if it doesn't already exist
        randomAccessFile = new RandomAccessFile(file, "rw");
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

        this.headerSyncInterval = builder.headerSyncInterval;
    }

    private MappedHeader getHeaderBuffer(ByteBuffer fileBuffer) {
        ByteBuffer headerBuffer = subBuffer(fileBuffer, 0, MappedHeader.HEADER_LENGTH);
        return new MappedHeader(headerBuffer);
    }

    private MappedEntries getEntriesBuffer(ByteBuffer fileBuffer) {
        ByteBuffer entriesBuffer = subBuffer(fileBuffer, MappedHeader.HEADER_LENGTH);
        return new MappedEntries(entriesBuffer);
    }

    public File getFile() {
        return file;
    }

    @Override
    public BufferQueue.Publisher publisher() throws IllegalStateException, IOException {
        if (publisher == null) {
            synchronized (this) {
                if (publisher == null) {
                    publisher = new Publisher();
                }
            }
        }
        return publisher;
    }

    @Override
    public BufferQueue.Consumer consumer() throws IllegalStateException, IOException {
        if (consumer == null) {
            synchronized (this) {
                if (consumer == null) {
                    consumer = new Consumer();
                }
            }
        }
        return consumer;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            if (publisher != null) publisher.close();
            if (consumer != null) consumer.close();
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

    void printBufferSkeleton() {
        System.out.println("ConsumeCursor: " + consumeCursor.get() + " PublishCursor: " + publishCursor.get());
        for (long i = consumeCursor.get(); i < publishCursor.get();) {
            MappedBufferQueueEntry entry = mappedEntries.getEntry(i);
            System.out.println("Cursor: " + i +
                    ", nextCursor: " + entry.nextCursor +
                    ", isPublished: " + entry.isPublished() +
                    ", isConsumed: " + entry.isConsumed());
            i = entry.nextCursor;
        }
    }

    public class Publisher implements BufferQueue.Publisher {

        private final FileLock fileLock;
        private final ScheduledExecutorService executorService;

        public Publisher() throws IOException {
            fileLock = mappedHeader.lockPublishing();
            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(new Syncer(), headerSyncInterval, headerSyncInterval, TimeUnit.MILLISECONDS);
        }

        @Override
        public BufferQueue bufferQueue() {
            return MappedBufferQueue.this;
        }

        /**
         * Claim the next entry in the buffer queue, ensuring that it spans across the given number of blocks. <br/><br/>
         * <p/>
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
                long maxNumEntries = maxNumEntries() - (n % maxNumEntries());
                while (maxNumEntries < numBlocks) {
                    if (publishCursor.compareAndSet(n, n + maxNumEntries)) {
                        mappedEntries.makeEntry(n, (byte) maxNumEntries).markConsumed();
                    }
                    else {
                        n = publishCursor.get();
                        maxNumEntries = maxNumEntries() - (n % maxNumEntries());
                    }
                }
                for (long i = n; i < n + numBlocks;) {
                    MappedBufferQueueEntry entry = mappedEntries.getEntry(i);
                    if (entry.isPublished() && !entry.isConsumed()) {
                        return Optional.absent();
                    }
                    i = entry.nextCursor;
                }
            }
            while (!publishCursor.compareAndSet(n, n + numBlocks));

            MappedBufferQueueEntry entry = mappedEntries.makeEntry(n, numBlocks);
            if (entry.nextCursor - entry.cursor < numBlocks) {
                entry.markConsumed();
                return claim(numBlocks);
            }

            return Optional.of(entry);
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
                    + (dataPlusMetadataSize % blockSize != 0 ? 1 : 0));
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

        void syncCursor() {
            mappedHeader.commitPublishCursor(publishCursor.get());
        }

        public void close() throws IOException {
            syncCursor();
            executorService.shutdownNow();
            fileLock.release();
        }

        class Syncer implements Runnable {
            @Override
            public void run() {
                syncCursor();
            }
        }
    }

    public class Consumer implements BufferQueue.Consumer {

        private final FileLock fileLock;
        private final ScheduledExecutorService executorService;
        private volatile long publishCursorVal = publishCursor.get();

        public Consumer() throws IOException {
            fileLock = mappedHeader.lockConsumption();
            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(new Syncer(), headerSyncInterval, headerSyncInterval, TimeUnit.MILLISECONDS);
        }

        @Override
        public BufferQueue bufferQueue() {
            return MappedBufferQueue.this;
        }

        public void forwardConsumeCursor() {
            long consumeCursorVal;
            while (consumeCursor.get() < publishCursorVal) {
                consumeCursorVal = consumeCursor.get();
                MappedBufferQueueEntry entry = mappedEntries.getEntry(consumeCursorVal);
                if (!entry.isConsumed()) {
                    break;
                }
                consumeCursor.compareAndSet(consumeCursorVal, entry.nextCursor);
            }
        }

        @Override
        public Optional<MappedBufferQueueEntry> peek() {
            checkNotClosed();

            forwardConsumeCursor();
            long consumeCursorVal = consumeCursor.get();
            if (consumeCursorVal < publishCursorVal) {
                MappedBufferQueueEntry entry = mappedEntries.getEntry(consumeCursorVal);
                if (entry.isPublished() && !entry.isConsumed()) {
                    return Optional.of(entry);
                }
            }
            return Optional.absent();
        }

        @Override
        public List<MappedBufferQueueEntry> peek(int n) {
            checkNotClosed();
            List<MappedBufferQueueEntry> bufferQueueEntries = Lists.newArrayList();

            forwardConsumeCursor();
            long consumeCursorVal = consumeCursor.get();
            long nextCursorVal = consumeCursorVal;
            for (int i = 0; i < Math.min(n, publishCursorVal - consumeCursorVal); i++) {
                MappedBufferQueueEntry entry = mappedEntries.getEntry(nextCursorVal);
                if (!entry.isPublished()) break;
                if (!entry.isConsumed()) {
                    bufferQueueEntries.add(entry);
                }
                nextCursorVal = entry.nextCursor;
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

        void syncCursor() {
            mappedHeader.commitConsumeCursor(consumeCursor.get());
            publishCursorVal = mappedHeader.readPublishCursor();
        }

        public void close() throws IOException {
            syncCursor();
            executorService.shutdownNow();
            fileLock.release();
        }

        class Syncer implements Runnable {
            @Override
            public void run() {
                syncCursor();
            }
        }
    }

    public static int metadataOverhead() {
        return MappedEntries.OFFSET_ENTRY + MappedBufferQueueEntry.OFFSET_ENTRY + BufferQueueEntry.metadataOverhead();
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

        FileLock lockPublishing() throws IOException {
            return fileChannel.lock(OFFSET_PUBLISH_CURSOR, Long.SIZE, false);
        }

        FileLock lockConsumption() throws IOException {
            return fileChannel.lock(OFFSET_CONSUME_CURSOR, Long.SIZE, false);
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
            int endOffset = (int) (((cursor + numBlocks - 1) % capacity) * blockSize);

            if (endOffset < offset) {
                numBlocks = (byte) (capacity - (offset / blockSize));
            }

            ByteBuffer buf = subBuffer(entriesBuffer, offset, numBlocks * blockSize);
            buf.put(OFFSET_NUM_BLOCKS, numBlocks);
            buf = subBuffer(buf, OFFSET_ENTRY);
            return new MappedBufferQueueEntry(buf, cursor, cursor + numBlocks);
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
            byte numBlocks = buf.get(OFFSET_NUM_BLOCKS);
            buf = subBuffer(buf, OFFSET_ENTRY, numBlocks * blockSize - OFFSET_ENTRY);
            return new MappedBufferQueueEntry(buf, cursor + numBlocks);
        }
    }

    public class MappedBufferQueueEntry extends BufferQueueEntry {

        @VisibleForTesting static final int OFFSET_CURSOR = 0;
        @VisibleForTesting static final int OFFSET_ENTRY = OFFSET_CURSOR + Long.SIZE;

        private static final long CURSOR_UNPUBLISHED = -1l;
        private static final long CURSOR_CONSUMED = -2l;
        private static final long CURSOR_SKIPPED = -2l;

        private final ByteBuffer buf;
        private final long cursor;

        private final long nextCursor;

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
        MappedBufferQueueEntry(ByteBuffer buf, long cursor, long nextCursor) {
            super(subBuffer(buf, OFFSET_ENTRY));
            this.buf = buf;
            this.cursor = cursor;
            this.nextCursor = nextCursor;
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
        MappedBufferQueueEntry(ByteBuffer buf, long nextCursor) {
            super(subBuffer(buf, OFFSET_ENTRY));
            this.buf = buf;
            this.cursor = buf.getLong(OFFSET_CURSOR);
            this.nextCursor = nextCursor;
        }

        @Override
        public void markPublished() {
            writeCursor(cursor);
        }

        @Override
        public boolean isPublished() {
            return readCursor() != CURSOR_UNPUBLISHED;
        }

        @Override
        public void markConsumed() {
            writeCursor(CURSOR_CONSUMED);
            consumer.forwardConsumeCursor();
        }

        @Override
        public boolean isConsumed() {
            return readCursor() == CURSOR_CONSUMED;
        }

        public void markSkipped() { writeCursor(CURSOR_SKIPPED); }

        public boolean isSkipped() { return readCursor() == CURSOR_SKIPPED; }

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
        try {
            buf = buf.duplicate();
            buf.position(start);
            buf = buf.slice();
            buf.limit(length);
            buf.rewind();
        }
        catch (IllegalArgumentException e) {
            System.out.println("start: " + start + " length: " + length + " buf.limit():" + buf.limit());
            throw e;
        }
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
