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

package com.flipkart.iris.bufferqueue;

import com.google.common.base.Optional;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.List;

/**
 * An API for a queue that is intended to be used as a local
 * buffer for data before it is sent to remote systems. <br/><br/>
 *
 * Publishing and consumption must be completely thread-safe. <br/><br/>
 *
 * Each entry in a <code>BufferQueue</code> is represented by
 * a {@link BufferQueueEntry}. <br/><br/>
 *
 * Enqueuing/publishing data to the queue is a 3-step process:
 * <ol>
 *      <li>"Claim" a new entry using the method {@link Publisher#claim()}</li>
 *      <li>Write the data to the returned {@link BufferQueueEntry} using any method that it offers</li>
 *      <li>Mark the entry as published by calling {@link BufferQueueEntry#markPublishedUnconsumed()}</li>
 * </ol>
 *
 * Alternatively, calling {@link Publisher#publish(byte[] data)} will
 * internally do all these steps automatically. <br/><br/>
 *
 * Multiple {@link Publisher#claim()}/{@link Publisher#publish(byte[] data)} calls could
 * be happening in parallel and entries must be made available for consumption
 * in the order in which they were claimed. <br/><br/>
 *
 * Dequeueing/consuming data from the queue is also a 3-step process:
 * <ol>
 *      <li>Get the next entry to be consumed from the queue by calling {@link Consumer#peek()}</li>
 *      <li>Read and consume the data from the returned {@link BufferQueueEntry}</li>
 *      <li>Mark the entry as consumed by calling {@link BufferQueueEntry#markConsumed()}</li>
 * </ol>
 *
 * Consumption using the above process will be ordered -- messages will be
 * consumed in the same order in which they were claimed/published. Until a message is
 * marked as consumed, the next message will not be returned. Thus it does not
 * make sense to consume in parallel -- the same entry will be delivered to multiple
 * queues. <br/><br/>
 *
 * There is also a batch consume API in the form of the method {@link Consumer#peek(int n)}
 * that allows consumption of multiple entries (up to <code>n</code>) at the same time.
 * This can also be used to parallelize consumption -- get a batch of entries and then
 * hand them off to a separate set of worker threads to process. Please note that this
 * would imply that entries would be processed out-of-order. <br/>
 *
 * @see com.flipkart.iris.bufferqueue.BufferQueueEntry
 *
 */
public interface BufferQueue {

    Publisher publisher() throws IllegalStateException, IOException;
    Consumer consumer() throws IllegalStateException, IOException;

    void close() throws IOException;

    public interface Publisher {

        BufferQueue bufferQueue() throws IOException;

        /**
         * Claim the next entry in the buffer queue. <br/>
         *
         * Important: The publisher **must** call {@link BufferQueueEntry#markPublishedUnconsumed()}
         * on the returned {@link BufferQueueEntry}, otherwise no consumer
         * will be able to move past this point. <br/><br/>
         *
         * {@link BufferQueueEntry#markPublishedUnconsumed()} **must** be called even if writing data
         * to the buffer failed. It is recommended that this method be called in a
         * <code>finally</code> block; for example:
         *
         * <pre>
         * <code>
         *      BufferQueueEntry entry = bufferQueue.next();
         *      try {
         *          // entry.set(data);
         *      }
         *      finally {
         *          entry.markPublishedUnconsumed();
         *      }
         * </code>
         * </pre>
         *
         * @return The claimed entry. The reference will be absent in the Optional
         *          if claiming failed (may happen if the buffer is full, for example).
         * @see BufferQueueEntry#markPublishedUnconsumed()
         */
        Optional<? extends BufferQueueEntry> claim() throws IOException;

        /**
         * Claim the next entry in the buffer queue to write data of given size. <br/><br/>
         *
         * Similar care needs to be taken when using this method as that which is required when using {@link #claim()}. See
         * the docs for that to understand these care instructions.
         *
         * @see #claim()
         */
        Optional<? extends BufferQueueEntry> claimFor(int dataSize) throws IOException;

        /**
         * A higher level helper method to do all the 3-steps of publishing
         * to the BufferQueue in a single method call. This claims a new entry,
         * writes data to the entry and then marks it as published.
         *
         * @param data The data to publish as an entry.
         * @return
         * @throws BufferOverflowException If the given data does not fit in a single entry.
         * @see #claim()
         */
        boolean publish(byte[] data) throws BufferOverflowException, IOException;
    }

    public interface Consumer {

        BufferQueue bufferQueue() throws IOException;

        /**
         * Return the next consumable entry from the BufferQueue. <br/><br/>
         *
         * Note that until the returned entry is marked as consumed, the same
         * entry will be returned for each call, even if different threads make
         * the call. <br/><br/>
         *
         * Thus consumers **must** call {@link BufferQueueEntry#markConsumed} on the returned
         * {@link BufferQueueEntry}. It is recommended that this method be
         * called in a <code>finally</code> block; for example:
         *
         * <pre>
         * <code>
         *      BufferQueueEntry entry = bufferQueue.consume();
         *      try {
         *          // consume the data from entry
         *      }
         *      finally {
         *          entry.markConsumed();
         *      }
         * </code>
         * </pre>
         *
         * @return The consumable {@link BufferQueueEntry}, null if
         *          no consumable entries are currently available or if the next
         *          entry is corrupted for any unknown reason.
         * @see BufferQueueEntry#markConsumed()
         */
        Optional<? extends BufferQueueEntry> peek() throws IOException;

        /**
         * Return the next (up to) <code>n</code> consumable entries from the BufferQueue. <br/><br/>
         *
         * Less than <code>n</code> entries (including <code>zero</code> entries) may be
         * returned based on how many entries are currently available. <br/><br/>
         *
         * The same set of contracts as specified in {@link #peek()} apply to this
         * method as well. <br/><br/>
         *
         * The returned messages may be marked as consumed out of order. This allows the
         * consumer to parallelize consumption of the messages (over a thread-pool, for
         * example).
         *
         * @param n The number of entries to return.
         * @return A list of up to <code>n</code> entries.
         * @see #peek()
         */
        List<? extends BufferQueueEntry> peek(int n) throws IOException;

        Optional<byte[]> consume() throws IOException;
        List<byte[]> consume(int n) throws IOException;
    }

    /**
     * BufferQueue implementations may have a max size of data that they accept.
     * Attempting to data with sizes greater than this will result in a
     * {@link BufferOverflowException}.
     *
     * @return The maximum length of data this BufferQueue can accomodate.
     */
    int maxDataLength() throws IOException;

    /**
     * BufferQueue implementations may have a upper limit on the number of
     * unconsumed messages they will hold.
     *
     * @return The max number of unconsumed messages this BufferQueue will hold.
     */
    long maxNumEntries();

    /**
     * Returns the number of unconsumed entries that this BufferQueue currently
     * holds. <br/><br/>
     *
     * This number may be returned on a "best-effort" basis and may only be
     * indicative instead of being accurate. This happens mainly because there
     * may be claimed but as yet unpublished entries as well as entries that
     * have been marked as consumed out of order (when using bulk consumption).

     * @return The current -- approximate -- size of the queue.
     */
    long size() throws IOException;

    /**
     * Check if the BufferQueue is "full" that is if it has any more
     * maxNumEntries to accept newer entries before any more entries are
     * consumed. <br/><br/>
     *
     * For certain implementations of BufferQueue it may be more efficient
     * to call this method than call figure this out by calling
     * {@link #maxNumEntries()} and {@link #size()}.
     *
     * @return <code>true</code> if the BufferQueue is full, <code>false</code> otherwise
     * @see #maxNumEntries()
     */
    boolean isFull() throws IOException;

    /**
     * Check if the BufferQueue has any consumable entries. This may be
     * an approximation and it is possible that even if this returns true
     * there are no consumable entries or vice versa. <br/>
     *
     * For certain implementations of BufferQueue it may be more efficient
     * to call this method than call figure this out by calling {@link #size()}.
     *
     * @return <code>true</code> if the BufferQueue is empty, <code>false</code> otherwise
     * @see #size()
     */
    boolean isEmpty() throws IOException;

    public static class ClosedBufferQueueException extends RuntimeException {
        public ClosedBufferQueueException() {
            super("Attempting to use a closed BufferQueue");
        }
    }
}
