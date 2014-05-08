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

import com.flipkart.iris.bufferqueue.BufferQueueEntry;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;

class MappedEntries {
    private final ByteBuffer entriesBuffer;
    public final int blockSize;
    public final long capacity;

    MappedEntries(ByteBuffer entriesBuffer, MappedHeader mappedHeader) {
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
    ByteBuffer getMessageBuffer(int offset, int numBlocks) {
        ByteBuffer msgBuf = entriesBuffer.duplicate();
        msgBuf.position(offset + 1);
        msgBuf = msgBuf.slice();
        msgBuf.limit(blockSize * numBlocks);
        return msgBuf;
    }

    @VisibleForTesting
    @NotNull
    BufferQueueEntry makeEntry(long cursor, int numBlocks) {
        // TODO: throw exception if numBlocks < Byte.MAX_VALUE?
        int offset = (int) ((cursor % capacity) * blockSize); // TODO: wrong, because we're casting to int
        entriesBuffer.put(offset, (byte) numBlocks);
        return new BufferQueueEntry(getMessageBuffer(offset, numBlocks), cursor);
    }

    @VisibleForTesting
    @NotNull
    BufferQueueEntry makeEntry(long cursor) {
        return makeEntry(cursor, 1);
    }

    @VisibleForTesting
    @NotNull
    BufferQueueEntry getEntry(long cursor) {
        int offset = (int) ((cursor % capacity) * blockSize); // TODO: wrong, because we're casting to int
        int numBlocks = entriesBuffer.get(offset);
        return new BufferQueueEntry(getMessageBuffer(offset, numBlocks));
    }
}
