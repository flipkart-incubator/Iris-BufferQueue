package com.flipkart.iris.bufferqueue.mmapped;

import com.flipkart.iris.bufferqueue.BufferQueue;
import com.flipkart.iris.bufferqueue.BufferQueueEntry;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.List;

public class MappedDirBufferQueue implements BufferQueue {

    private final File dir;
    private final int blockSize;
    private final int fileSize;
    private final int maxFiles;

    private volatile Publisher publisher;
    private volatile Consumer consumer;

    private MappedBufferQueue oldestBufferQueue;
    private MappedBufferQueue newestBufferQueue;
    private boolean isClosed;

    public MappedDirBufferQueue(File dir, int blockSize, int fileSize, int maxFiles) throws IOException {
        this.dir = dir;
        this.blockSize = blockSize;
        this.fileSize = fileSize;
        this.maxFiles = maxFiles;

        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Couldn't create directory");
            }
        }

        File newestFile = getNewestFile();
        if (newestFile == null) {
            createNewBufferQueue();
        }
        else {
            newestBufferQueue = new MappedBufferQueue.Builder(newestFile)
                    .headerSyncInterval(1)
                    .build();
        }

        updateOldestBufferQueue();
    }

    private File getNewestFile() {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return null;
        }
        Arrays.sort(files);
        return files[files.length - 1];
    }

    private File getOldestFile() {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return null;
        }
        Arrays.sort(files);
        return files[0];
    }

    private synchronized void createNewBufferQueue() throws IOException {
        if (newestBufferQueue != null && newestBufferQueue != oldestBufferQueue) {
            newestBufferQueue.close();
        }

        File file = new File(dir, Long.toString(System.nanoTime()));
        newestBufferQueue = new MappedBufferQueue.Builder(file)
                .formatIfNotExists(fileSize, blockSize)
                .headerSyncInterval(1)
                .build();
    }

    private synchronized void updateOldestBufferQueue() throws IOException {
        if (oldestBufferQueue != null) {
            if (!oldestBufferQueue.isEmpty()) {
                return;
            }
            if (oldestBufferQueue == newestBufferQueue) {
                return;
            }
            oldestBufferQueue.close();
            File oldFile = oldestBufferQueue.getFile();
            if (!oldFile.delete()) {
                throw new IOException("Unable to delete old file");
            }
        }

        File oldestFile = getOldestFile();
        if (newestBufferQueue != null && oldestFile.equals(newestBufferQueue.getFile())) {
            oldestBufferQueue = newestBufferQueue;
        }
        else {
            oldestBufferQueue = new MappedBufferQueue.Builder(oldestFile)
                    .headerSyncInterval(1)
                    .build();
        }
    }

    private BufferQueue.Publisher getMappedBufferQueuePublisher() throws IOException {
        if (newestBufferQueue.isFull()) {
            synchronized (this) {
                if (newestBufferQueue.isFull()) {
                    createNewBufferQueue();
                }
            }
        }
        return newestBufferQueue.publisher();
    }

    private BufferQueue.Consumer getMappedBufferQueueConsumer() throws IOException {
        if (oldestBufferQueue.isEmpty()) {
            synchronized (this) {
                if (oldestBufferQueue.isEmpty()) {
                    updateOldestBufferQueue();
                }
            }
        }
        return oldestBufferQueue.consumer();
    }

    @Override
    public Publisher publisher() throws IllegalStateException, IOException {
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
    public Consumer consumer() throws IllegalStateException, IOException {
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
            if (oldestBufferQueue != null) {
                oldestBufferQueue.close();
            }
            if (newestBufferQueue != null && newestBufferQueue != oldestBufferQueue) {
                newestBufferQueue.close();
            }
        }
        isClosed = true;
    }

    public class Publisher implements BufferQueue.Publisher {

        @Override
        public BufferQueue bufferQueue() throws IOException {
            return getMappedBufferQueuePublisher().bufferQueue();
        }

        @Override
        public Optional<? extends BufferQueueEntry> claim() throws IOException {
            Optional<? extends BufferQueueEntry> claim =  getMappedBufferQueuePublisher().claim();
            if (!claim.isPresent()) {
                synchronized (this) {
                    claim = getMappedBufferQueuePublisher().claim();
                    if (!claim.isPresent()) {
                        createNewBufferQueue();
                        claim = getMappedBufferQueuePublisher().claim();
                    }
                }
            }
            return claim;
        }

        @Override
        public Optional<? extends BufferQueueEntry> claimFor(int dataSize) throws IOException {
            Optional<? extends BufferQueueEntry> claim =  getMappedBufferQueuePublisher().claimFor(dataSize);
            if (!claim.isPresent()) {
                synchronized (this) {
                    claim = getMappedBufferQueuePublisher().claimFor(dataSize);
                    if (!claim.isPresent()) {
                        createNewBufferQueue();
                        claim = getMappedBufferQueuePublisher().claimFor(dataSize);
                    }
                }
            }
            return claim;
        }

        @Override
        public boolean publish(byte[] data) throws BufferOverflowException, IOException {
            boolean publish = getMappedBufferQueuePublisher().publish(data);
            if (!publish) {
                synchronized (this) {
                    publish = getMappedBufferQueuePublisher().publish(data);
                    if (!publish) {
                        createNewBufferQueue();
                        publish = getMappedBufferQueuePublisher().publish(data);
                    }
                }
            }
            return publish;
        }

    }

    public class Consumer implements BufferQueue.Consumer {

        @Override
        public BufferQueue bufferQueue() throws IOException {
            return getMappedBufferQueueConsumer().bufferQueue();
        }

        @Override
        public Optional<? extends BufferQueueEntry> peek() throws IOException {
            return getMappedBufferQueueConsumer().peek();
        }

        @Override
        public List<? extends BufferQueueEntry> peek(int n) throws IOException {
            return getMappedBufferQueueConsumer().peek(n);
        }

        @Override
        public Optional<byte[]> consume() throws IOException {
            return getMappedBufferQueueConsumer().consume();
        }

        @Override
        public List<byte[]> consume(int n) throws IOException {
            return getMappedBufferQueueConsumer().consume(n);
        }
    }

    @Override
    public int maxDataLength() throws IOException {
        return Math.max(getMappedBufferQueueConsumer().bufferQueue().maxDataLength(),
                getMappedBufferQueuePublisher().bufferQueue().maxDataLength());
    }

    @Override
    public long maxNumEntries() {
        return 0;
    }

    @Override
    public long size() throws IOException {
        return getMappedBufferQueuePublisher().bufferQueue().size()
                + getMappedBufferQueueConsumer().bufferQueue().size();
    }

    @Override
    public boolean isFull() throws IOException {
        return getMappedBufferQueuePublisher().bufferQueue().isFull();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return getMappedBufferQueueConsumer().bufferQueue().isEmpty()
                && getMappedBufferQueuePublisher().bufferQueue().isEmpty();
    }

    public static void main(String[] args) throws IOException {
        MappedDirBufferQueue bufferQueue = new MappedDirBufferQueue(new File("/tmp/bqtest"), 1024, 10240, 5);
        long startTime = System.nanoTime();
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 100; i++) {
                System.out.println("Publishing " + i);
                boolean publish = bufferQueue.publisher().publish(Integer.toString(i).getBytes());
                System.out.println(publish);
            }
            for (int i = 0; i < 100; i++) {
                Optional<byte[]> msg = bufferQueue.consumer().consume();
                if (msg.isPresent()) {
                    System.out.println(new String(msg.get()));
                }
                else {
                    break;
                }
            }
        }
        long endTime = System.nanoTime();
        System.out.println("Time Taken: " + (endTime - startTime)/1000/1000);
    }
}
