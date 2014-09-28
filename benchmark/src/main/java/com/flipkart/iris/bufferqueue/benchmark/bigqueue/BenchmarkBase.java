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

package com.flipkart.iris.bufferqueue.benchmark.bigqueue;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.flipkart.iris.bufferqueue.benchmark.Constants.*;

@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(value = Scope.Group)
public abstract class BenchmarkBase {

    @Param({ZERO_LENGTH_MESSAGE_KEY, VERY_SHORT_MESSAGE_KEY, SHORT_MESSAGE_KEY, MEDIUM_MESSAGE_KEY, LONG_MESSAGE_KEY, VERY_LONG_MESSAGE_KEY})
    private String msgKey;

    protected byte[] msg;
    private File file;
    protected IBigQueue bigQueue;

    @Setup(Level.Trial)
    public void init() throws IOException {
        msg = getMessage(msgKey);
        file = File.createTempFile("bufferqueue-bigqueue-benchmark", Long.toString(System.nanoTime()));
        file.delete();
        bigQueue = new BigQueueImpl(file.getPath(), "demo");
    }

    @TearDown(Level.Iteration)
    public void gc() throws IOException {
        bigQueue.gc();
    }

    @TearDown(Level.Trial)
    public void destroy() throws IOException {
        bigQueue.close();
        file.delete();
    }
}
