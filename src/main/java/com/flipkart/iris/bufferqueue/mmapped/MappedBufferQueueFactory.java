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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class MappedBufferQueueFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MappedBufferQueueFactory.class);

    public static final String FILE_EXTENSION = ".mbq";
    public static final String FILENAME_FORMAT_STRING = "%s-%d-%d" + FILE_EXTENSION;
    public static final int PAGE_SIZE = 4 * 1024;

    /**
     * Create an instance of MappedBufferQueue, backed by the given file.
     *
     * The file must already exist and be formatted correctly. This can be ensured using {@link #format(java.io.File, int, long)}.
     *
     * @param file The backing file for the memory-mapped BufferQueue.
     * @return An instance of MappedBufferQueue.
     * @throws FileNotFoundException If the file does not already exist.
     * @throws IOException In case of any I/O error when trying to access the file.
     */
    public static MappedBufferQueue getInstance(File file) throws FileNotFoundException, IOException {
        if (!file.exists()) {
            throw new FileNotFoundException("File " + file + " does not exist!");
        }

        ByteBuffer fileBuf = mapFile(file, file.length());
        return new MappedBufferQueue(file, fileBuf);
    }

    /**
     * Format a file to be used as a backing file for a MappedBufferQueue.
     *
     * The file must not already exist, it will be created.
     *
     * @param file The file to format.
     * @param maxDataLength The maximum length of any single data unit within this file.
     * @param numMessages The maximum number of unconsumed messages that this queue can contain.
     * @throws IllegalStateException If the given file already exists.
     * @throws IOException In case of any I/O error when trying to create or write to the file.
     */
    public static void format(File file, int maxDataLength, long numMessages) throws IllegalStateException, IOException {
        if (file.exists()) {
            throw new IllegalStateException("File " + file + " already exists!");
        }

        long fileSize = MappedBufferQueue.fileSize(maxDataLength, numMessages);
        touchFile(file, fileSize);
        ByteBuffer fileBuf = mapFile(file, fileSize);
        MappedBufferQueue.format(fileBuf, maxDataLength);
    }

    /**
     * A convention-driven factory method that will create and format the backing file if it doesn't already exist.
     *
     * This automatically constructs a name for the file that includes the important parameters like maxDataLength and
     * numMessages. So if these parameters are changes during an application restart, a new file will get created.
     * Warning: This could imply that there remain unconsumed data in the old file.
     *
     * @param directory The directory in which to create the file.
     * @param name A base name for the file.
     * @param maxDataLength The maximum length of any single data unit within this file.
     * @param numMessages The maximum number of unconsumed messages that this queue can contain.
     * @return An instance of MappedBufferQueue.
     * @throws IOException In case of any I/O error when trying to create, write to or, access the file.
     */
    public static MappedBufferQueue getInstance(File directory, String name, int maxDataLength, long numMessages)
            throws IOException {

        File file = new File(directory, String.format(FILENAME_FORMAT_STRING, name, maxDataLength, numMessages));

        if (!file.exists()) {
            format(file, maxDataLength, numMessages);
        }

        return getInstance(file);
    }

    public static List<MappedBufferQueue> getInstances(File directory) throws IOException {
        File[] files = directory.listFiles();
        Preconditions.checkNotNull(files, "Error fetching list of files in given directory", directory);

        List<MappedBufferQueue> bufferQueues = Lists.newArrayList();
        for (File file : files) {
            if (file.getName().endsWith(FILE_EXTENSION)) {
                bufferQueues.add(getInstance(file));
            }
        }

        return bufferQueues;
    }

    private static void touchFile(File file, long fileSize) throws IOException {
        // TODO: find a better way to create a file, one that will work for files larger than 2gb

        Preconditions.checkArgument(fileSize > PAGE_SIZE, "fileSize must be at least %s", PAGE_SIZE);

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fc = raf.getChannel();

        MappedByteBuffer fileBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        fileBuf.order(ByteOrder.nativeOrder()); // TODO: do we really need this?
    }

    private static MappedByteBuffer mapFile(File file, long fileSize) throws IOException {
        Preconditions.checkArgument(fileSize > PAGE_SIZE, "fileSize must be at least %s", PAGE_SIZE);

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fc = raf.getChannel();

        MappedByteBuffer fileBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        fileBuf.order(ByteOrder.nativeOrder()); // TODO: do we really need this?
        return fileBuf;
    }
}
