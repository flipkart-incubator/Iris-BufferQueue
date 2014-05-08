package com.flipkart.iris.bufferqueue.mmapped;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Helper {
    public static final int PAGE_SIZE = 4 * 1024;

    public static void createFile(File file, long fileSize) throws IOException {
        Preconditions.checkArgument(fileSize > PAGE_SIZE, "fileSize must be at least %s", PAGE_SIZE);

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fc = raf.getChannel();

        MappedByteBuffer fileBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        fileBuf.order(ByteOrder.nativeOrder()); // TODO: do we really need this?
    }

    public static MappedByteBuffer mapFile(File file, long fileSize) throws IOException {
        Preconditions.checkArgument(fileSize > PAGE_SIZE, "fileSize must be at least %s", PAGE_SIZE);

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fc = raf.getChannel();

        MappedByteBuffer fileBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        fileBuf.order(ByteOrder.nativeOrder()); // TODO: do we really need this?
        return fileBuf;
    }
}
