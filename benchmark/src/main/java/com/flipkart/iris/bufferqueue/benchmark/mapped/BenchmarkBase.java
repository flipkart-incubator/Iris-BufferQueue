/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.flipkart.iris.bufferqueue.benchmark.mapped;

import com.flipkart.iris.bufferqueue.mmapped.MappedBufferQueue;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.flipkart.iris.bufferqueue.benchmark.Constants.*;

@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(value = Scope.Benchmark)
public class BenchmarkBase {

    @Param({"256", "1024", "4096"})
    private String blockSizeStr;

    @Param({ZERO_LENGTH_MESSAGE_KEY, VERY_SHORT_MESSAGE_KEY, SHORT_MESSAGE_KEY, MEDIUM_MESSAGE_KEY})
    private String msgKey;

    protected byte[] msg;
    private File file;
    protected MappedBufferQueue bufferQueue;

    //@AuxCounters
    @State(Scope.Thread)
    public static class OperationStatus {
        public long produceSuccessful;
        public long produceFailed;
        public long consumed;

        @Setup(Level.Iteration)
        public void reset() {
            produceSuccessful = 0;
            produceFailed = 0;
            consumed = 0;
        }

        public void setProduceStatus(boolean status) {
            if (status) produceSuccessful++;
            else produceFailed++;
        }

        public void setConsumeStatus(boolean status) {
            if (status) consumed++;
        }

        public void addConsumed(int consumed) {
            this.consumed += consumed;
        }
    }

    //@Setup(Level.Iteration)
    //@TearDown(Level.Iteration)
    public void printStatus() {
        bufferQueue.printBufferSkeleton("!!!");
    }

    @Setup(Level.Trial)
    public void init() throws IOException {
        msg = getMessage(msgKey);
        file = File.createTempFile("bufferqueue-benchmark", Long.toString(System.nanoTime()));
        file.delete();
        bufferQueue = new MappedBufferQueue.Builder(this.file)
                .formatIfNotExists(1024 * 1024 * 1024, Integer.valueOf(blockSizeStr))
                .headerSyncInterval(1)
                .build();
    }

    @TearDown(Level.Trial)
    public void destroy() throws IOException {
        bufferQueue.close();
        file.delete();
    }
}
