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

package org.apache.druid.indexing.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.seekablestream.SeekableStreamRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaRecordSupplier extends SeekableStreamRecordSupplier<Integer, Long>
{
  private static final EmittingLogger log = new EmittingLogger(KafkaRecordSupplier.class);
  private static final Random RANDOM = ThreadLocalRandom.current();


  private final Map<String, String> consumerProperties;
  private final long pollTimeoutMillis;
  private final Map<String, KafkaConsumer<byte[], byte[]>> kafkaConsumerMap = new ConcurrentHashMap<>();

  private class KafkaPartitionResource extends SeekableStreamRecordSupplier<Integer, Long>.SeekablePartitionResource
  {

    private final KafkaConsumer<byte[], byte[]> consumer;

    KafkaPartitionResource(StreamPartition<Integer> streamPartition, KafkaConsumer<byte[], byte[]> consumer)
    {
      super(streamPartition);
      consumer.assign(Collections.singletonList(new TopicPartition(
          streamPartition.getStream(),
          streamPartition.getPartitionId()
      )));
      this.consumer = consumer;
    }

    @Override
    protected void fetchRecords() throws InterruptedException
    {

      ConsumerRecords<byte[], byte[]> polledRecords = consumer.poll(pollTimeoutMillis);

      for (ConsumerRecord<byte[], byte[]> kafkaRecord : polledRecords) {

        OrderedPartitionableRecord<Integer, Long> record = new OrderedPartitionableRecord<>(
            kafkaRecord.topic(),
            kafkaRecord.partition(),
            kafkaRecord.offset(),
            ImmutableList.of(kafkaRecord.value())
        );

        if (log.isTraceEnabled()) {
          log.trace(
              "Stream[%s] / partition[%s] / sequenceNum[%s] / bufferRemainingCapacity[%d]: %s",
              record.getStream(),
              record.getPartitionId(),
              record.getSequenceNumber(),
              records.remainingCapacity(),
              record.getData().stream().map(String::new).collect(Collectors.toList())
          );
        }

        // If the buffer was full and we weren't able to add the message, grab a new stream iterator starting
        // from this message and back off for a bit to let the buffer drain before retrying.
        if (!records.offer(record, recordBufferOfferTimeout, TimeUnit.MILLISECONDS)) {
          log.warn(
              "OrderedPartitionableRecord buffer full, storing iterator and retrying in [%,dms]",
              recordBufferFullWait
          );

          consumer.seek(new TopicPartition(kafkaRecord.topic(), kafkaRecord.partition()), kafkaRecord.offset());

          rescheduleRunnable(recordBufferFullWait);
          return;
        }

      }
    }
  }


  public KafkaRecordSupplier(
      Map<String, String> consumerProperties,
      int fetchDelayMillis,
      int fetchThreads,
      int recordBufferSize,
      int recordBufferOfferTimeout,
      int recordBufferFullWait,
      long pollTimeoutMillis
  )
  {
    super(
        fetchDelayMillis,
        fetchThreads,
        recordBufferSize,
        recordBufferOfferTimeout,
        recordBufferFullWait
    );
    this.consumerProperties = consumerProperties;
    this.pollTimeoutMillis = pollTimeoutMillis;
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    checkIfClosed();

    Preconditions.checkArgument(sequenceNumber != null);

    KafkaPartitionResource resource = (KafkaPartitionResource) partitionResources.get(partition);
    if (resource == null) {
      throw new ISE("Partition [%s] has not been assigned", partition);
    }


    log.debug(
        "Seeking partition [%s] to [%s]",
        partition.getPartitionId(),
        sequenceNumber
    );

    resource.consumer.seek(new TopicPartition(partition.getStream(), partition.getPartitionId()), sequenceNumber);

    checkPartitionsStarted = true;
  }

  @Override
  public void seekAfter(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    seek(partition, sequenceNumber + 1);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> partitions)
  {
    checkIfClosed();
    partitions.forEach(partition -> {
      KafkaPartitionResource resource = (KafkaPartitionResource) partitionResources.get(partition);
      if (resource == null) {
        throw new ISE("Partition [%s] has not been assigned", partition);
      }

      resource.consumer.seekToBeginning(Collections.singletonList(new TopicPartition(
          partition.getStream(),
          partition.getPartitionId()
      )));

    });
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> partitions)
  {
    checkIfClosed();
    partitions.forEach(partition -> {
      KafkaPartitionResource resource = (KafkaPartitionResource) partitionResources.get(partition);
      if (resource == null) {
        throw new ISE("Partition [%s] has not been assigned", partition);
      }

      resource.consumer.seekToEnd(Collections.singletonList(new TopicPartition(
          partition.getStream(),
          partition.getPartitionId()
      )));

    });
  }


  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    checkIfClosed();

    KafkaPartitionResource resource = (KafkaPartitionResource) partitionResources.get(partition);
    if (resource == null) {
      throw new ISE("Partition [%s] has not been assigned", partition);
    }

    TopicPartition topicPartition = new TopicPartition(
        partition.getStream(),
        partition.getPartitionId()
    );

    seekToLatest(Collections.singleton(partition));
    return resource.consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    checkIfClosed();

    KafkaPartitionResource resource = (KafkaPartitionResource) partitionResources.get(partition);
    if (resource == null) {
      throw new ISE("Partition [%s] has not been assigned", partition);
    }

    TopicPartition topicPartition = new TopicPartition(
        partition.getStream(),
        partition.getPartitionId()
    );

    seekToLatest(Collections.singleton(partition));
    return resource.consumer.beginningOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
  }

  @Override
  public Long position(StreamPartition<Integer> partition)
  {
    checkIfClosed();

    KafkaPartitionResource resource = (KafkaPartitionResource) partitionResources.get(partition);
    if (resource == null) {
      throw new ISE("Partition [%s] has not been assigned", partition);
    }

    return resource.consumer.position(new TopicPartition(partition.getStream(), partition.getPartitionId()));
  }

  @Override
  public Set<Integer> getPartitionIds(String topic)
  {
    checkIfClosed();
    final Map<String, List<PartitionInfo>> topics = getKafkaConsumer(topic).listTopics();
    if (topics == null || !topics.containsKey(topic)) {
      throw new ISE("Topic [%s] is not found in KafkaConsumer's list of topics", topic);
    }
    return topics.get(topic).stream().map(PartitionInfo::partition).collect(Collectors.toSet());
  }

  @Override
  protected SeekablePartitionResource createPartitionResource(
      StreamPartition<Integer> streamPartition
  )
  {
    return new KafkaPartitionResource(
        streamPartition,
        getKafkaConsumer()
    );
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer(String topic)
  {
    if (!kafkaConsumerMap.containsKey(topic)) {
      kafkaConsumerMap.put(topic, getKafkaConsumer(topic));
    }

    return kafkaConsumerMap.get(topic);
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer()
  {
    final Properties props = new Properties();

    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("group.id", StringUtils.format("kafka-supervisor-%s", getRandomId()));

    props.putAll(consumerProperties);

    props.setProperty("enable.auto.commit", "false");

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }
}
