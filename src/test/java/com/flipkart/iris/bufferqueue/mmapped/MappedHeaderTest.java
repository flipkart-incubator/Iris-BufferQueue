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
    MappedHeader mappedHeader;

    @Before
    public void setUp() throws Exception {
        mappedHeader = new MappedHeader(ByteBuffer.allocate(MappedHeader.HEADER_LENGTH));
        mappedHeader.format(MAX_DATA_LENGTH);
    }

    @Test
    public void testFormat() throws Exception {
        assertEquals(MAX_DATA_LENGTH, mappedHeader.maxDataLength());
        assertEquals(1, mappedHeader.readCursor());
        assertEquals(1, mappedHeader.writeCursor());
    }

    @Test
    public void testWriteCursor() throws Exception {
        long newWriteCursor = mappedHeader.writeCursor() + 1;
        mappedHeader.writeCursor(newWriteCursor);
        assertEquals(newWriteCursor, mappedHeader.writeCursor());

        newWriteCursor = mappedHeader.writeCursor() + 1;
        mappedHeader.writeCursor(newWriteCursor);
        assertEquals(newWriteCursor, mappedHeader.writeCursor());
    }

    @Test
    public void testReadCursor() throws Exception {
        long newReadCursor = mappedHeader.readCursor() + 1;
        mappedHeader.readCursor(newReadCursor);
        assertEquals(newReadCursor, mappedHeader.readCursor());

        newReadCursor = mappedHeader.readCursor() + 1;
        mappedHeader.readCursor(newReadCursor);
        assertEquals(newReadCursor, mappedHeader.readCursor());
    }
}
