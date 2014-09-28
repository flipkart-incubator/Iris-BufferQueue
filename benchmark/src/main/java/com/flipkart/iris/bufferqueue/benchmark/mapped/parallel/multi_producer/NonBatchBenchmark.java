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

package com.flipkart.iris.bufferqueue.benchmark.mapped.parallel.multi_producer;

import com.flipkart.iris.bufferqueue.BufferQueueEntry;
import com.flipkart.iris.bufferqueue.benchmark.mapped.BenchmarkBase;
import com.google.common.base.Optional;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;

@State(Scope.Group)
@Threads(1)
public class NonBatchBenchmark extends BenchmarkBase {

    @Benchmark
    @Group("peek")
    @GroupThreads(2)
    public boolean testProduceForPeek(OperationStatus operationStatus) throws IOException {
        boolean publish = bufferQueue.publisher().publish(msg);
        operationStatus.setProduceStatus(publish);
        return publish;
    }

    @Benchmark
    @Group("peek")
    @GroupThreads(1)
    public boolean testPeek(OperationStatus operationStatus) throws IOException {
        Optional<? extends BufferQueueEntry> peek = bufferQueue.consumer().peek();
        if (peek.isPresent()) {
            peek.get().markConsumed();
            operationStatus.addConsumed(1);
        }
        return peek.isPresent();
    }

    @Benchmark
    @Group("consume")
    @GroupThreads(2)
    public boolean testProduceForConsume(OperationStatus operationStatus) throws IOException {
        boolean publish = bufferQueue.publisher().publish(msg);
        operationStatus.setProduceStatus(publish);
        return publish;
    }

    @Benchmark
    @Group("g")
    @GroupThreads(1)
    public boolean testConsume(OperationStatus operationStatus) throws IOException {
        Optional<byte[]> consume = bufferQueue.consumer().consume();
        if (consume.isPresent()) {
            operationStatus.addConsumed(1);
        }
        return consume.isPresent();
    }
}
