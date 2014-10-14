package com.flipkart.iris.bufferqueue.profiling;

import com.flipkart.iris.bufferqueue.mmapped.MappedBufferQueue;

import java.io.File;
import java.io.IOException;

public class CreateFile {
    public static void main(String[] args) throws IOException {
        String file = args[0];
        MappedBufferQueue bufferQueue = new MappedBufferQueue.Builder(new File(file))
                .formatIfNotExists(1024 * 1024 * 1024, 1024)
                .build();
        bufferQueue.printBufferSkeleton("after creation");
        bufferQueue.close();
    }
}
