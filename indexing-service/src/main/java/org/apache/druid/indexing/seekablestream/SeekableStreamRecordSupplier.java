/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.seekablestream;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class SeekableStreamRecordSupplier<partitionType, sequenceType>
    implements RecordSupplier<partitionType, sequenceType>
{
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamRecordSupplier.class);
  private static final long EXCEPTION_RETRY_DELAY_MS = 10000;

  protected abstract class SeekablePartitionResource
  {
    private final Object startLock = new Object();
    protected final StreamPartition<partitionType> streamPartition;

    private volatile boolean started;
    private volatile boolean stopRequested;

    protected SeekablePartitionResource(
        StreamPartition<partitionType> streamPartition
    )
    {
      this.streamPartition = streamPartition;
    }

    public void start()
    {
      synchronized (startLock) {
        if (started) {
          return;
        }

        log.info(
            "Starting scheduled fetch runnable for stream[%s] partition[%s]",
            streamPartition.getStream(),
            streamPartition.getPartitionId()
        );

        stopRequested = false;
        started = true;

        rescheduleRunnable(fetchDelayMillis);
      }
    }

    public void stop()
    {
      log.info(
          "Stopping scheduled fetch runnable for stream[%s] partition[%s]",
          streamPartition.getStream(),
          streamPartition.getPartitionId()
      );

      stopRequested = true;
    }

    private Runnable getRecordRunnable()
    {
      return () -> {
        if (stopRequested) {
          started = false;
          stopRequested = false;

          log.info("Worker for partition[%s] has been stopped", streamPartition.getPartitionId());
          return;
        }

        try {
          fetchRecords();
        }
        catch (Throwable e) {
          log.error(e, "getRecordRunnable exception, retrying in [%,dms]", EXCEPTION_RETRY_DELAY_MS);
          rescheduleRunnable(EXCEPTION_RETRY_DELAY_MS);
        }


      };
    }

    protected void rescheduleRunnable(long delayMillis)
    {
      if (started && !stopRequested) {
        scheduledExec.schedule(getRecordRunnable(), delayMillis, TimeUnit.MILLISECONDS);
      } else {
        log.info("Worker for partition[%s] has been stopped", streamPartition.getPartitionId());
      }
    }

    protected abstract void fetchRecords() throws InterruptedException;
  }

  private final ScheduledExecutorService scheduledExec;
  protected volatile boolean closed = false;
  protected volatile boolean checkPartitionsStarted = false;


  protected final int fetchDelayMillis;
  protected final int recordBufferOfferTimeout;
  protected final int recordBufferFullWait;
  protected final Map<StreamPartition<partitionType>, SeekablePartitionResource> partitionResources = new ConcurrentHashMap<>();
  protected final BlockingQueue<OrderedPartitionableRecord<partitionType, sequenceType>> records;

  public SeekableStreamRecordSupplier(
      int fetchDelayMillis,
      int fetchThreads,
      int recordBufferSize,
      int recordBufferOfferTimeout,
      int recordBufferFullWait
  )
  {
    this.fetchDelayMillis = fetchDelayMillis;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait;

    records = new LinkedBlockingQueue<>(recordBufferSize);

    log.info(
        "Creating fetch thread pool of size [%d] (Runtime.availableProcessors=%d)",
        fetchThreads,
        Runtime.getRuntime().availableProcessors()
    );

    scheduledExec = Executors.newScheduledThreadPool(
        fetchThreads, Execs.makeThreadFactory("SeekableStreamRecordSupplier-Worker-%d")
    );
  }


  @Override
  public void assign(Set<StreamPartition<partitionType>> collection)
  {
    checkIfClosed();

    collection.forEach(
        streamPartition -> partitionResources.putIfAbsent(
            streamPartition,
            createPartitionResource(streamPartition)
        )
    );

    Iterator<Map.Entry<StreamPartition<partitionType>, SeekablePartitionResource>> i = partitionResources.entrySet()
                                                                                                         .iterator();

    while (i.hasNext()) {
      Map.Entry<StreamPartition<partitionType>, SeekablePartitionResource> entry = i.next();
      if (!collection.contains(entry.getKey())) {
        i.remove();
        entry.getValue().stop();
      }
    }
  }

  protected abstract SeekablePartitionResource createPartitionResource(StreamPartition<partitionType> streamPartition);

  @Override
  public Collection<StreamPartition<partitionType>> getAssignment()
  {
    return partitionResources.keySet();
  }

  @Nullable
  @Override
  public OrderedPartitionableRecord<partitionType, sequenceType> poll(long timeout)
  {
    checkIfClosed();
    if (checkPartitionsStarted) {
      partitionResources.values().forEach(SeekablePartitionResource::start);
      checkPartitionsStarted = false;
    }

    try {
      while (true) {
        OrderedPartitionableRecord<partitionType, sequenceType> record = records.poll(timeout, TimeUnit.MILLISECONDS);
        if (record == null || partitionResources.containsKey(record.getStreamPartition())) {
          return record;
        } else if (log.isTraceEnabled()) {
          log.trace(
              "Skipping stream[%s] / partition[%s] / sequenceNum[%s] because it is not in current assignment",
              record.getStream(),
              record.getPartitionId(),
              record.getSequenceNumber()
          );
        }
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "InterruptedException");
      return null;
    }
  }

  @Override
  public void close()
  {
    if (this.closed) {
      return;
    }

    assign(ImmutableSet.of());

    scheduledExec.shutdown();

    try {
      if (!scheduledExec.awaitTermination(EXCEPTION_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
        scheduledExec.shutdownNow();
      }
    }
    catch (InterruptedException e) {
      log.info(e, "InterruptedException while shutting down");
    }

    this.closed = true;
  }


  protected void checkIfClosed()
  {
    if (closed) {
      throw new ISE("Invalid operation - SeekableStreamRecordSupplier has already been closed");
    }
  }
}
