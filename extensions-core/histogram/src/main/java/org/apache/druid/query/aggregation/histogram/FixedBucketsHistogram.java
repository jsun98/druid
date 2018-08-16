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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.codec.binary.Base64;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FixedBucketsHistogram
{
  private static final Logger log = new Logger(FixedBucketsHistogram.class);

  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();
  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();
  private static final byte SERIALIZATION_VERSION = 0x01;

  public enum OutlierHandlingMode
  {
    OVERFLOW,
    IGNORE,
    CLIP;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static OutlierHandlingMode fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }

    public byte[] getCacheKey()
    {
      return new byte[] {(byte) this.ordinal()};
    }
  }

  private double lowerLimit;
  private double upperLimit;
  private int numBuckets;

  private long upperOutlierCount = 0;
  private long lowerOutlierCount = 0;
  private long missingValueCount = 0;
  private long[] histogram;
  private double bucketSize;
  private OutlierHandlingMode outlierHandlingMode;

  private long count = 0;
  private double max = Double.NEGATIVE_INFINITY;
  private double min = Double.POSITIVE_INFINITY;

  public FixedBucketsHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      OutlierHandlingMode outlierHandlingMode
  )
  {
    Preconditions.checkArgument(upperLimit > lowerLimit, "Upper limit must be greater than lower limit.");
    Preconditions.checkArgument(numBuckets > 0, "Must have a positive number of buckets.");
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.numBuckets = numBuckets;
    this.outlierHandlingMode = outlierHandlingMode;

    this.histogram = new long[numBuckets];
    this.bucketSize = (upperLimit - lowerLimit) / numBuckets;
  }

  private FixedBucketsHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      OutlierHandlingMode outlierHandlingMode,
      long[] histogram,
      long count,
      double max,
      double min,
      long lowerOutlierCount,
      long upperOutlierCount,
      long missingValueCount
  )
  {
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.numBuckets = numBuckets;
    this.outlierHandlingMode = outlierHandlingMode;
    this.histogram = histogram;
    this.count = count;
    this.max = max;
    this.min = min;
    this.upperOutlierCount = upperOutlierCount;
    this.lowerOutlierCount = lowerOutlierCount;
    this.missingValueCount = missingValueCount;

    this.bucketSize = (upperLimit - lowerLimit) / numBuckets;
  }

  @VisibleForTesting
  public FixedBucketsHistogram getCopy()
  {
    return new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode,
        Arrays.copyOf(histogram, histogram.length),
        count,
        max,
        min,
        lowerOutlierCount,
        upperOutlierCount,
        missingValueCount
    );
  }

  private void handleOutlier(boolean exceededMax, double value)
  {
    switch (outlierHandlingMode) {
      case CLIP:
        if (exceededMax) {
          histogram[histogram.length - 1] += 1;
        } else {
          histogram[0] += 1;
        }
        count += 1;
        if (value > max) {
          max = value;
        }
        if (value < min) {
          min = value;
        }
        break;
      case OVERFLOW:
        if (exceededMax) {
          upperOutlierCount += 1;
        } else {
          lowerOutlierCount += 1;
        }
        break;
      case IGNORE:
        break;
      default:
        throw new ISE("Unknown outlier handling mode: " + outlierHandlingMode);
    }
  }

  public void incrementMissing()
  {
    missingValueCount++;
  }

  public void add(
      double value
  )
  {
    if (value < lowerLimit) {
      handleOutlier(false, value);
      return;
    }

    if (value > upperLimit) {
      handleOutlier(true, value);
      return;
    }

    count += 1;
    if (value > max) {
      max = value;
    }
    if (value < min) {
      min = value;
    }

    double valueRelativeToRange = value - lowerLimit;
    int targetBucket = (int) (valueRelativeToRange / bucketSize);

    if (targetBucket >= histogram.length) {
      targetBucket = histogram.length - 1;
    }
    histogram[targetBucket] += 1;
  }

  @JsonProperty
  public double getLowerLimit()
  {
    return lowerLimit;
  }

  @JsonProperty
  public double getUpperLimit()
  {
    return upperLimit;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public long getUpperOutlierCount()
  {
    return upperOutlierCount;
  }

  @JsonProperty
  public long getLowerOutlierCount()
  {
    return lowerOutlierCount;
  }

  @JsonProperty
  public long getMissingValueCount()
  {
    return missingValueCount;
  }

  @JsonProperty
  public long[] getHistogram()
  {
    return histogram;
  }

  @JsonProperty
  public double getBucketSize()
  {
    return bucketSize;
  }

  @JsonProperty
  public long getCount()
  {
    return count;
  }

  @JsonProperty
  public double getMax()
  {
    return max;
  }

  @JsonProperty
  public double getMin()
  {
    return min;
  }

  @JsonProperty
  public OutlierHandlingMode getOutlierHandlingMode()
  {
    return outlierHandlingMode;
  }

  public FixedBucketsHistogram combineHistogram(FixedBucketsHistogram otherHistogram)
  {
    if (otherHistogram == null) {
      return this;
    }

    missingValueCount += otherHistogram.getMissingValueCount();

    if (bucketSize == otherHistogram.getBucketSize() &&
        lowerLimit == otherHistogram.getLowerLimit() &&
        upperLimit == otherHistogram.getUpperLimit()) {
      return combineHistogramSameBuckets(otherHistogram);
    } else {
      return combineHistogramDifferentBuckets(otherHistogram);
    }
  }

  public FixedBucketsHistogram combineHistogramSameBuckets(FixedBucketsHistogram otherHistogram)
  {
    long[] otherHistogramArray = otherHistogram.getHistogram();
    for (int i = 0; i < numBuckets; i++) {
      histogram[i] += otherHistogramArray[i];
    }

    count += otherHistogram.getCount();
    max = Math.max(max, otherHistogram.getMax());
    min = Math.min(min, otherHistogram.getMin());
    lowerOutlierCount += otherHistogram.getLowerOutlierCount();
    upperOutlierCount += otherHistogram.getUpperOutlierCount();
    missingValueCount += otherHistogram.getMissingValueCount();

    return this;
  }

  public FixedBucketsHistogram combineHistogramDifferentBuckets(FixedBucketsHistogram otherHistogram)
  {
    if (otherHistogram.getLowerLimit() >= upperLimit) {
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          upperOutlierCount += otherHistogram.getCount();
          break;
        case CLIP:
          histogram[histogram.length - 1] += otherHistogram.getCount();
          count += otherHistogram.getCount();
          max = Math.max(max, otherHistogram.getMax());
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else if (otherHistogram.getUpperLimit() <= lowerLimit) {
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          lowerOutlierCount += otherHistogram.getCount();
          break;
        case CLIP:
          histogram[0] += otherHistogram.getCount();
          count += otherHistogram.getCount();
          min = Math.min(min, otherHistogram.getMin());
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else {
      simpleInterpolateMerge(otherHistogram);
    }
    missingValueCount += otherHistogram.getMissingValueCount();

    return this;
  }

  public double getCumulativeCount(double cutoff, boolean fromStart)
  {
    int cutoffBucket = (int) ((cutoff - lowerLimit) / bucketSize);
    double count = 0;

    if (fromStart) {
      for (int i = 0; i <= cutoffBucket; i++) {
        if (i == cutoffBucket) {
          double bucketStart = i * bucketSize + lowerLimit;
          double partialCount = ((cutoff - bucketStart) / bucketSize) * histogram[i];
          count += partialCount;
        } else {
          count += histogram[i];
        }
      }
    } else {
      for (int i = cutoffBucket; i < histogram.length; i++) {
        if (i == cutoffBucket) {
          double bucketEnd = ((i + 1) * bucketSize) + lowerLimit;
          double partialCount = ((bucketEnd - cutoff) / bucketSize) * histogram[i];
          count += partialCount;
        } else {
          count += histogram[i];
        }
      }
    }
    return count;
  }

  public void simpleInterpolateMergeHandleOutliers(
      FixedBucketsHistogram otherHistogram,
      double rangeStart,
      double rangeEnd
  )
  {
    if (rangeStart == lowerLimit) {
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          lowerOutlierCount += Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
          lowerOutlierCount += otherHistogram.getLowerOutlierCount();
          upperOutlierCount += Math.round(otherHistogram.getUpperOutlierCount());
          break;
        case CLIP:
          histogram[0] += Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
          histogram[0] += otherHistogram.getLowerOutlierCount();
          histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
          if (otherHistogram.getCount() > 0) {
            min = Math.min(min, otherHistogram.getMin());
            max = Math.max(max, otherHistogram.getMax());
          }
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    }
    if (rangeEnd == upperLimit) {
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          upperOutlierCount += Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));
          upperOutlierCount += otherHistogram.getUpperOutlierCount();
          lowerOutlierCount += otherHistogram.getLowerOutlierCount();
          break;
        case CLIP:
          histogram[histogram.length - 1] += Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));
          histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
          histogram[0] += otherHistogram.getLowerOutlierCount();
          if (otherHistogram.getCount() > 0) {
            max = Math.max(max, otherHistogram.getMax());
            min = Math.min(min, otherHistogram.getMin());
          }
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    }

    if (rangeStart > lowerLimit && rangeEnd < upperLimit) {
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          upperOutlierCount += otherHistogram.getUpperOutlierCount();
          lowerOutlierCount += otherHistogram.getLowerOutlierCount();
          break;
        case CLIP:
          histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
          histogram[0] += otherHistogram.getLowerOutlierCount();
          if (otherHistogram.getCount() > 0) {
            max = Math.max(max, otherHistogram.getMax());
            min = Math.min(min, otherHistogram.getMin());
          }
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    }
  }

  // assumes that values are uniformly distributed within each bucket in the other histogram
  public void simpleInterpolateMerge(FixedBucketsHistogram otherHistogram)
  {
    double rangeStart = Math.max(lowerLimit, otherHistogram.getLowerLimit());
    double rangeEnd = Math.min(upperLimit, otherHistogram.getUpperLimit());

    int myCurBucket = (int) ((rangeStart - lowerLimit) / bucketSize);
    double myNextCursorBoundary = (myCurBucket + 1) * bucketSize + lowerLimit;
    double myCursor = rangeStart;

    int theirCurBucket = (int) ((rangeStart - otherHistogram.getLowerLimit()) / otherHistogram.getBucketSize());
    double theirNextCursorBoundary = (theirCurBucket + 1) * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();
    double theirCursor = rangeStart;

    double intraBucketStride = otherHistogram.getBucketSize() / otherHistogram.getHistogram()[theirCurBucket];

    myNextCursorBoundary = Math.min(myNextCursorBoundary, rangeEnd);
    theirNextCursorBoundary = Math.min(theirNextCursorBoundary, rangeEnd);

    simpleInterpolateMergeHandleOutliers(otherHistogram, rangeStart, rangeEnd);

    double theirCurrentLowerBucketBoundary = theirCurBucket * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();

    while (myCursor < rangeEnd) {
      double needToConsume = myNextCursorBoundary - myCursor;
      double canConsumeFromOtherBucket = theirNextCursorBoundary - theirCursor;
      double toConsume = Math.min(needToConsume, canConsumeFromOtherBucket);

      // consume one of our bucket's worth of data from the other histogram
      while (needToConsume > 0) {
        /*
        log.info(
            "TOCONSUME: %s, TheirC: %s, TCB: %s, TNCB: %s, MyC: %s, MCB: %s, MNCB: %s",
            toConsume,
            theirCursor,
            theirCurBucket,
            theirNextCursorBoundary,
            myCursor,
            myCurBucket,
            myNextCursorBoundary
        );
        */

        double fractional = toConsume / otherHistogram.getBucketSize();
        double fractionalCount = otherHistogram.getHistogram()[theirCurBucket] * fractional;

        double cursorFrac = (theirCursor - theirCurrentLowerBucketBoundary) / otherHistogram.getBucketSize();
        double value = cursorFrac * otherHistogram.bucketSize + theirCurrentLowerBucketBoundary;

        if (Math.round(fractionalCount) > 0) {
          /*
          log.info(
              "-------- ADD VALUE ---- MYBUCKET: %s, FC: %s",
              myCurBucket,
              Math.round(fractionalCount)
          );
          */

          // in this chunk from the other histogram bucket, calculate min and max interpolated values that would be added
          double minStride = Math.ceil(
              (theirCursor - (theirCurBucket * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit()))
              / intraBucketStride
          );

          minStride *= intraBucketStride;
          minStride += theirCurrentLowerBucketBoundary;
          if (minStride >= theirCursor) {
            /*
            log.info(
                "--------SET MIN------ FRACCOUNT: %s, IVAL: %s, CURSORFRAC: %s, FRAC: %s, IBS: %s, MSN: %s",
                fractionalCount,
                value,
                cursorFrac,
                fractional,
                intraBucketStride,
                minStride
            );
            */
            min = Math.min(minStride, min);
          }

          double maxStride = Math.floor(
              (theirCursor + toConsume - (theirCurBucket * otherHistogram.getBucketSize()
                                          + otherHistogram.getLowerLimit())) / intraBucketStride
          );
          maxStride *= intraBucketStride;
          maxStride += theirCurrentLowerBucketBoundary;
          if (maxStride < theirCursor + toConsume) {
            /*
            log.info(
                "--------SET MAX------ FRACCOUNT: %s, IVAL: %s, CURSORFRAC: %s, FRAC: %s, IBS: %s, XSN: %s",
                fractionalCount,
                value,
                cursorFrac,
                fractional,
                intraBucketStride,
                maxStride
            );
            */
            max = Math.max(maxStride, max);
          }
        }

        histogram[myCurBucket] += Math.round(fractionalCount);
        count += Math.round(fractionalCount);

        needToConsume -= toConsume;
        theirCursor += toConsume;
        myCursor += toConsume;


        if (theirCursor >= rangeEnd) {
          break;
        }

        // we've crossed buckets in the other histogram
        if (theirCursor >= theirNextCursorBoundary) {
          theirCurBucket += 1;
          intraBucketStride = otherHistogram.getBucketSize() / otherHistogram.getHistogram()[theirCurBucket];
          theirCurrentLowerBucketBoundary = theirCurBucket * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();
          theirNextCursorBoundary = (theirCurBucket + 1) * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();
          theirNextCursorBoundary = Math.min(theirNextCursorBoundary, rangeEnd);
        }


        canConsumeFromOtherBucket = theirNextCursorBoundary - theirCursor;
        toConsume = Math.min(needToConsume, canConsumeFromOtherBucket);
      }

      if (theirCursor >= rangeEnd) {
        break;
      }

      myCurBucket += 1;
      myNextCursorBoundary = (myCurBucket + 1) * bucketSize + lowerLimit;
      myNextCursorBoundary = Math.min(myNextCursorBoundary, rangeEnd);
    }

    //log.info("FINAL TC: " + theirCursor + " TCB: " + theirCurBucket + ", TNCB: " + theirNextCursorBoundary);
  }

  // Based off PercentileBuckets code from Netflix Spectator: https://github.com/Netflix/spectator
  public float[] percentilesFloat(double[] pcts)
  {
    float[] results = new float[pcts.length];
    long total = count;

    int pctIdx = 0;

    long prev = 0;
    double prevP = 0.0;
    double prevB = lowerLimit;
    for (int i = 0; i < numBuckets; ++i) {
      long next = prev + histogram[i];
      double nextP = 100.0 * next / total;
      double nextB = (i + 1) * bucketSize + lowerLimit;
      while (pctIdx < pcts.length && nextP >= pcts[pctIdx]) {
        double f = (pcts[pctIdx] - prevP) / (nextP - prevP);
        results[pctIdx] = (float) (f * (nextB - prevB) + prevB);
        ++pctIdx;
      }
      if (pctIdx >= pcts.length) {
        break;
      }
      prev = next;
      prevP = nextP;
      prevB = nextB;

      //log.info("PREV: %s, PREVP: %s, PREVB: %s, PCTIDX: %s", prev, prevP, prevB, pctIdx);
    }

    return results;
  }

  public byte[] toBytesFull(boolean withHeader)
  {
    int size = getFullStorageSize(numBuckets);
    if (withHeader) {
      size += Byte.BYTES + Integer.BYTES;
    }
    ByteBuffer buf = ByteBuffer.allocate(size);
    toBytesFullHelper(buf, withHeader);
    return buf.array();
  }

  private void toBytesFullHelper(ByteBuffer buf, boolean withHeader)
  {
    if (withHeader) {
      buf.put(SERIALIZATION_VERSION);
      buf.putInt(-1); // no LZ4 compression
    }
    buf.putDouble(lowerLimit);
    buf.putDouble(upperLimit);
    buf.putInt(numBuckets);
    buf.putInt(outlierHandlingMode.ordinal());

    buf.putLong(count);
    buf.putLong(lowerOutlierCount);
    buf.putLong(upperOutlierCount);
    buf.putLong(missingValueCount);

    buf.putDouble(max);
    buf.putDouble(min);

    buf.asLongBuffer().put(histogram);
    buf.position(buf.position() + Long.BYTES * histogram.length);
  }

  public static int getFullStorageSize(int numBuckets)
  {
    return Double.BYTES +
           Double.BYTES +
           Integer.BYTES +
           Integer.BYTES +
           Long.BYTES * 4 +
           Double.BYTES * 2 +
           Long.BYTES * numBuckets;
  }

  public static FixedBucketsHistogram fromBytes(byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return fromBytes(buf);
  }

  public static FixedBucketsHistogram fromBytes(ByteBuffer buf)
  {
    byte serializationVersion = buf.get();
    Preconditions.checkArgument(
        serializationVersion == SERIALIZATION_VERSION,
        StringUtils.format("Only serialization version %s is supported.", SERIALIZATION_VERSION)
    );
    int uncompressedSize = buf.getInt();
    if (uncompressedSize == -1) {
      return fromBytesFull(buf);
    } else {
      return fromBytesLZ4(buf, uncompressedSize);
    }
  }

  public static FixedBucketsHistogram fromBytesFull(byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return fromBytesFull(buf);
  }

  @JsonValue
  public byte[] toBytesLZ4()
  {
    byte[] src = toBytesFull(false);
    int maxStorageSize = LZ4_COMPRESSOR.maxCompressedLength(getFullStorageSize(numBuckets));
    byte[] dst = new byte[maxStorageSize];
    int compressedSize = LZ4_COMPRESSOR.compress(
        src,
        0,
        src.length,
        dst,
        0,
        maxStorageSize
    );
    byte[] dstCompressed = new byte[Byte.BYTES + Integer.BYTES + compressedSize];
    System.arraycopy(dst, 0, dstCompressed, Byte.BYTES + Integer.BYTES, compressedSize);
    ByteBuffer wrapped = ByteBuffer.wrap(dstCompressed, 0, Byte.BYTES + Integer.BYTES + compressedSize);
    wrapped.rewind();
    wrapped.put(SERIALIZATION_VERSION);
    wrapped.putInt(src.length);
    return wrapped.array();
  }

  private static FixedBucketsHistogram fromBytesLZ4(ByteBuffer buf, int uncompressedSize)
  {
    ByteBuffer dst = ByteBuffer.allocate(uncompressedSize);
    LZ4_DECOMPRESSOR.decompress(buf.duplicate(), dst);
    dst.rewind();
    return fromBytesFull(dst);
  }

  /**
   * Constructs an ApproximateHistogram object from the given dense byte-buffer representation
   *
   * @param buf ByteBuffer to construct an ApproximateHistogram from
   *
   * @return ApproximateHistogram constructed from the given ByteBuffer
   */
  private static FixedBucketsHistogram fromBytesFull(ByteBuffer buf)
  {
    double lowerLimit = buf.getDouble();
    double upperLimit = buf.getDouble();
    int numBuckets = buf.getInt();
    OutlierHandlingMode outlierHandlingMode = OutlierHandlingMode.values()[buf.getInt()];

    long count = buf.getLong();
    long lowerOutlierCount = buf.getLong();
    long upperOutlierCount = buf.getLong();
    long missingValueCount = buf.getLong();

    double max = buf.getDouble();
    double min = buf.getDouble();

    long histogram[] = new long[numBuckets];
    buf.asLongBuffer().get(histogram);
    buf.position(buf.position() + Long.BYTES * histogram.length);

    return new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode,
        histogram,
        count,
        max,
        min,
        lowerOutlierCount,
        upperOutlierCount,
        missingValueCount
    );
  }

  public static FixedBucketsHistogram fromBase64(String encodedHistogram)
  {
    byte[] asBytes = Base64.decodeBase64(encodedHistogram.getBytes(StandardCharsets.UTF_8));
    return fromBytes(asBytes);
  }

  public String toBase64(boolean useLZ4)
  {
    byte[] asBytes = useLZ4 ? toBytesLZ4() : toBytesFull(true);
    return StringUtils.fromUtf8(Base64.encodeBase64(asBytes));
  }

  public Map<String, Object> finalizedForm()
  {
    Map<String, Object> mymap = new HashMap<>();
    mymap.put("lowerLimit", lowerLimit);
    mymap.put("upperLimit", upperLimit);
    mymap.put("numBuckets", numBuckets);
    mymap.put("upperOutlierCount", upperOutlierCount);
    mymap.put("lowerOutlierCount", lowerOutlierCount);
    mymap.put("missingValueCount", missingValueCount);
    mymap.put("histogram", histogram);
    mymap.put("outlierHandlingMode", outlierHandlingMode);
    mymap.put("count", count);
    mymap.put("max", max);
    mymap.put("min", min);
    return mymap;
  }

  @Override
  public String toString()
  {
    return "{" +
           "lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           ", numBuckets=" + numBuckets +
           ", upperOutlierCount=" + upperOutlierCount +
           ", lowerOutlierCount=" + lowerOutlierCount +
           ", missingValueCount=" + missingValueCount +
           ", histogram=" + Arrays.toString(histogram) +
           ", outlierHandlingMode=" + outlierHandlingMode +
           ", count=" + count +
           ", max=" + max +
           ", min=" + min +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FixedBucketsHistogram that = (FixedBucketsHistogram) o;
    return Double.compare(that.getLowerLimit(), getLowerLimit()) == 0 &&
           Double.compare(that.getUpperLimit(), getUpperLimit()) == 0 &&
           getNumBuckets() == that.getNumBuckets() &&
           getUpperOutlierCount() == that.getUpperOutlierCount() &&
           getLowerOutlierCount() == that.getLowerOutlierCount() &&
           getMissingValueCount() == that.getMissingValueCount() &&
           Double.compare(that.getBucketSize(), getBucketSize()) == 0 &&
           getCount() == that.getCount() &&
           Double.compare(that.max, max) == 0 &&
           Double.compare(that.min, min) == 0 &&
           Arrays.equals(getHistogram(), that.getHistogram()) &&
           getOutlierHandlingMode() == that.getOutlierHandlingMode();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getLowerLimit(),
        getUpperLimit(),
        getNumBuckets(),
        getUpperOutlierCount(),
        getLowerOutlierCount(),
        getMissingValueCount(),
        Arrays.hashCode(getHistogram()),
        getBucketSize(),
        getOutlierHandlingMode(),
        getCount(),
        max,
        min
    );
  }
}
