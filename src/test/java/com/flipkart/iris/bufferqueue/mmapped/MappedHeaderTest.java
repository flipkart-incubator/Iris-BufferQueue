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

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class MappedHeaderTest {

    public static final int MAX_DATA_LENGTH = 2048;
    MappedBufferQueue.MappedHeader mappedHeader;

    @Before
    public void setUp() throws Exception {
        mappedHeader = new MappedBufferQueue.MappedHeader(ByteBuffer.allocate(MappedBufferQueue.MappedHeader.HEADER_LENGTH));
        mappedHeader.format(MAX_DATA_LENGTH);
    }

    @Test
    public void testFormat() throws Exception {
        assertEquals(MAX_DATA_LENGTH, mappedHeader.blockSize());
        assertEquals(0, mappedHeader.readConsumeCursor());
        assertEquals(0, mappedHeader.readPublishCursor());
    }

    @Test
    public void testPublishCursor_Read_And_Commit() throws Exception {
        long newPublishCursor = mappedHeader.readPublishCursor() + 1;
        mappedHeader.commitPublishCursor(newPublishCursor);
        assertEquals(newPublishCursor, mappedHeader.readPublishCursor());

        newPublishCursor = mappedHeader.readPublishCursor() + 1;
        mappedHeader.commitPublishCursor(newPublishCursor);
        assertEquals(newPublishCursor, mappedHeader.readPublishCursor());
    }

    @Test
    public void testConsumeCursor_Read_And_Commit() throws Exception {
        long newConsumeCursor = mappedHeader.readConsumeCursor() + 1;
        mappedHeader.commitConsumeCursor(newConsumeCursor);
        assertEquals(newConsumeCursor, mappedHeader.readConsumeCursor());

        newConsumeCursor = mappedHeader.readConsumeCursor() + 1;
        mappedHeader.commitConsumeCursor(newConsumeCursor);
        assertEquals(newConsumeCursor, mappedHeader.readConsumeCursor());
    }
}
