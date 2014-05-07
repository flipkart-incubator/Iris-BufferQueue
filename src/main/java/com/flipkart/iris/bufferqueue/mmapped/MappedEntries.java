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
    public final int maxEntryLength;
    public final long capacity;

    MappedEntries(ByteBuffer entriesBuffer, MappedHeader mappedHeader) {
        this.entriesBuffer = entriesBuffer;
        this.maxEntryLength = BufferQueueEntry.calculateEntryLength(mappedHeader.maxDataLength());
        this.capacity = entriesBuffer.limit() / maxEntryLength;
    }

    public void format() {
        // TODO: think about whether we really need the following block
        long numMessages = capacity;
        for (int i = 0; i < numMessages; i++) {
            makeEntry(i).set(new byte[0]);
        }
    }

    @VisibleForTesting
    ByteBuffer getMessageBuffer(long index) {
        int maxMessageLength = maxEntryLength;
        long offset = (index % capacity) * maxEntryLength;

        ByteBuffer msgBuf = entriesBuffer.duplicate();
        msgBuf.position((int) offset); // TODO: wrong, because we're casting to int
        msgBuf = msgBuf.slice();
        msgBuf.limit(maxMessageLength);

        return msgBuf;
    }

    @VisibleForTesting
    @NotNull
    BufferQueueEntry makeEntry(long cursor) {
        return new BufferQueueEntry(getMessageBuffer(cursor), cursor);
    }

    @VisibleForTesting
    @NotNull
    BufferQueueEntry getEntry(long cursor) {
        return new BufferQueueEntry(getMessageBuffer(cursor));
    }
}
