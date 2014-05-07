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

public class MappedEntriesTest {

    public static final int HEADER_LENGTH = 4096;
    public static final int MAX_DATA_LENGTH = 2048;
    public static final int ENTRIES_SIZE = 4096 * 10;
    MappedEntries mappedEntries;

    @Before
    public void setUp() throws Exception {
        MappedHeader mappedHeader = new MappedHeader(ByteBuffer.allocate(HEADER_LENGTH));
        mappedHeader.format(MAX_DATA_LENGTH);
        mappedEntries = new MappedEntries(ByteBuffer.allocate(ENTRIES_SIZE), mappedHeader);
    }

    @Test
    public void testConstruction() throws Exception {
//        assertEquals();
    }

    @Test
    public void testFormat() throws Exception {

    }

    @Test
    public void testGetMessageBuffer() throws Exception {

    }

    @Test
    public void testMakeEntry() throws Exception {

    }

    @Test
    public void testGetEntry() throws Exception {

    }
}
