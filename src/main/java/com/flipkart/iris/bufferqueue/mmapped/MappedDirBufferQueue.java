//package com.flipkart.iris.bufferqueue.mmapped;
//
//import com.flipkart.iris.bufferqueue.BufferQueue;
//import com.flipkart.iris.bufferqueue.BufferQueueEntry;
//import com.google.common.base.Optional;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.BufferOverflowException;
//import java.util.Arrays;
//import java.util.List;
//
//public class MappedDirBufferQueue implements BufferQueue {
//
//    private final File dir;
//    private final int blockSize;
//    private final int fileSize;
//    private final int maxFiles;
//
//    private MappedBufferQueue oldestBufferQueue;
//    private MappedBufferQueue newestBufferQueue;
//
//    public MappedDirBufferQueue(File dir, int blockSize, int fileSize, int maxFiles) throws IOException {
//        this.dir = dir;
//        this.blockSize = blockSize;
//        this.fileSize = fileSize;
//        this.maxFiles = maxFiles;
//
//        if (!dir.exists()) {
//            if (!dir.mkdirs()) {
//                throw new IOException("Couldn't create directory");
//            }
//        }
//
//        updateNewestBufferQueue();
//        updateOldestBufferQueue();
//    }
//
//    private File getNewestFile() {
//        File[] files = dir.listFiles();
//        if (files == null || files.length == 0) {
//            return null;
//        }
//        Arrays.sort(files);
//        return files[files.length-1];
//    }
//
//    private synchronized void updateNewestBufferQueue() throws IOException {
//        if (newestBufferQueue != null && newestBufferQueue != oldestBufferQueue) {
//            newestBufferQueue.close();
//        }
//
//        File newestFile = getNewestFile();
//        if (newestFile == null) {
//            createNewBufferQueue();
//        }
//        else {
//            if (oldestBufferQueue != null && newestFile.equals(oldestBufferQueue.getFile())) {
//                newestBufferQueue = oldestBufferQueue;
//            }
//            else {
//                newestBufferQueue = new MappedBufferQueue.Builder(newestFile)
//                        .headerSyncInterval(1)
//                        .build();
//            }
//        }
//    }
//
//    private synchronized void createNewBufferQueue() throws IOException {
//        if (newestBufferQueue != null && newestBufferQueue != oldestBufferQueue) {
//            newestBufferQueue.close();
//        }
//
//        File file = new File(dir, Long.toString(System.nanoTime()));
//        newestBufferQueue = new MappedBufferQueue.Builder(file)
//                .formatIfNotExists(fileSize, blockSize)
//                .headerSyncInterval(1)
//                .build();
//    }
//
//    private File getOldestFile() {
//        File[] files = dir.listFiles();
//        if (files == null || files.length == 0) {
//            return null;
//        }
//        Arrays.sort(files);
//        return files[0];
//    }
//
//    private synchronized void updateOldestBufferQueue() throws IOException {
//        if (oldestBufferQueue != null && oldestBufferQueue != newestBufferQueue) {
//            oldestBufferQueue.close();
//            File oldFile = oldestBufferQueue.getFile();
//            if (!oldFile.delete()) {
//                throw new IOException("Unable to delete old file");
//            }
//        }
//
//        File oldestFile = getOldestFile();
//        if (newestBufferQueue != null && oldestFile.equals(newestBufferQueue.getFile())) {
//            oldestBufferQueue = newestBufferQueue;
//        }
//        else {
//            oldestBufferQueue = new MappedBufferQueue.Builder(oldestFile)
//                    .headerSyncInterval(1)
//                    .build();
//        }
//    }
//
//    private MappedBufferQueue getMappedBufferQueueForPublish() throws IOException {
//        if (newestBufferQueue.isFull()) {
//            synchronized (this) {
//                if (newestBufferQueue.isFull()) {
//                    createNewBufferQueue();
//                }
//            }
//        }
//        return newestBufferQueue;
//    }
//
//    private MappedBufferQueue getMappedBufferQueueForConsume() throws IOException {
//        if (oldestBufferQueue.isEmpty()) {
//            synchronized (this) {
//                if (oldestBufferQueue.isEmpty()) {
//                    updateOldestBufferQueue();
//                }
//            }
//        }
//        return oldestBufferQueue;
//    }
//
//    @Override
//    public void close() throws IOException {
//        oldestBufferQueue.close();
//        if (newestBufferQueue != oldestBufferQueue) {
//            newestBufferQueue.close();
//        }
//    }
//
//    @Override
//    public Optional<? extends BufferQueueEntry> claim() throws IOException {
//        Optional<MappedBufferQueue.MappedBufferQueueEntry> claim = getMappedBufferQueueForPublish().claim();
//        if (!claim.isPresent()) {
//            synchronized (this) {
//                claim = getMappedBufferQueueForPublish().claim();
//                if (!claim.isPresent()) {
//                    createNewBufferQueue();
//                    claim = getMappedBufferQueueForPublish().claim();
//                }
//            }
//        }
//        return claim;
//    }
//
//    @Override
//    public Optional<? extends BufferQueueEntry> claimFor(int dataSize) throws IOException {
//        Optional<MappedBufferQueue.MappedBufferQueueEntry> claim = getMappedBufferQueueForPublish().claimFor(dataSize);
//        if (!claim.isPresent()) {
//            synchronized (this) {
//                claim = getMappedBufferQueueForPublish().claimFor(dataSize);
//                if (!claim.isPresent()) {
//                    createNewBufferQueue();
//                    claim = getMappedBufferQueueForPublish().claimFor(dataSize);
//                }
//            }
//        }
//        return claim;
//    }
//
//    @Override
//    public boolean publish(byte[] data) throws BufferOverflowException, IOException {
//        boolean publish = getMappedBufferQueueForPublish().publish(data);
//        if (!publish) {
//            synchronized (this) {
//                publish = getMappedBufferQueueForPublish().publish(data);
//                if (!publish) {
//                    createNewBufferQueue();
//                    publish = getMappedBufferQueueForPublish().publish(data);
//                }
//            }
//        }
//        return publish;
//    }
//
//    @Override
//    public Optional<? extends BufferQueueEntry> peek() throws IOException {
//        return getMappedBufferQueueForConsume().peek();
//    }
//
//    @Override
//    public List<? extends BufferQueueEntry> peek(int n) throws IOException {
//        return getMappedBufferQueueForConsume().peek(n);
//    }
//
//    @Override
//    public Optional<byte[]> consume() throws IOException {
//        return getMappedBufferQueueForConsume().consume();
//    }
//
//    @Override
//    public List<byte[]> consume(int n) throws IOException {
//        return getMappedBufferQueueForConsume().consume(n);
//    }
//
//    @Override
//    public int maxDataLength() throws IOException {
//        return Math.max(getMappedBufferQueueForConsume().maxDataLength(), getMappedBufferQueueForPublish().maxDataLength());
//    }
//
//    @Override
//    public long maxNumEntries() {
//        return 0;
//    }
//
//    @Override
//    public long size() throws IOException {
//        return getMappedBufferQueueForPublish().size() + getMappedBufferQueueForConsume().size();
//    }
//
//    @Override
//    public boolean isFull() throws IOException {
//        return getMappedBufferQueueForPublish().isFull();
//    }
//
//    @Override
//    public boolean isEmpty() throws IOException {
//        return getMappedBufferQueueForConsume().isEmpty() && getMappedBufferQueueForPublish().isEmpty();
//    }
//
//    public static void main(String[] args) throws IOException {
//        MappedDirBufferQueue bufferQueue = new MappedDirBufferQueue(new File("/tmp/bqtest"), 1024, 10240, 5);
//        long startTime = System.nanoTime();
//        for (int j = 0; j < 100; j++) {
//            for (int i = 0; i < 100; i++) {
//                bufferQueue.publish(Integer.toString(i).getBytes());
//            }
//            for (int i = 0; i < 100; i++) {
//                List<byte[]> msg = bufferQueue.consume(100);
//                if (msg.size() == 0) {
//                    break;
//                    //System.out.println(new String(msg.get()));
//                }
//            }
//        }
//        long endTime = System.nanoTime();
//        System.out.println("Time Taken: " + (endTime - startTime)/1000/1000);
//    }
//}
