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

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.druid.java.util.common.logger.Logger;
import org.jboss.netty.util.internal.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class FixedBucketsHistogramTest
{
  private static final Logger log = new Logger(FixedBucketsHistogramTest.class);

  static final float[] VALUES2 = {23, 19, 10, 16, 36, 2, 1, 9, 32, 30, 45, 46};

  static final float[] VALUES3 = {
      20, 16, 19, 27, 17, 20, 18, 20, 28, 14, 17, 21, 20, 21, 10, 25, 23, 17, 21, 18,
      14, 20, 18, 12, 19, 20, 23, 25, 15, 22, 14, 17, 15, 23, 23, 15, 27, 20, 17, 15
  };
  static final float[] VALUES4 = {
      27.489f, 3.085f, 3.722f, 66.875f, 30.998f, -8.193f, 5.395f, 5.109f, 10.944f, 54.75f,
      14.092f, 15.604f, 52.856f, 66.034f, 22.004f, -14.682f, -50.985f, 2.872f, 61.013f,
      -21.766f, 19.172f, 62.882f, 33.537f, 21.081f, 67.115f, 44.789f, 64.1f, 20.911f,
      -6.553f, 2.178f
  };
  static final float[] VALUES5 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static final float[] VALUES6 = {
      1f, 1.5f, 2f, 2.5f, 3f, 3.5f, 4f, 4.5f, 5f, 5.5f, 6f, 6.5f, 7f, 7.5f, 8f, 8.5f, 9f, 9.5f, 10f
  };

  // Based on the example from https://metamarkets.com/2013/histograms/
  // This dataset can make getQuantiles() return values exceeding max
  // for example: q=0.95 returns 25.16 when max=25
  static final float[] VALUES7 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 12, 12, 15, 20, 25, 25, 25};

  protected FixedBucketsHistogram buildHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode,
      float[] values
  )
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );

    for (float v : values) {
      h.add(v);
    }
    return h;
  }

  @Test
  public void testOffer()
  {
    log.info("testOffer");
    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES2
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES2.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES2[i];
    }

    Arrays.sort(doubles);

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferRandoms()
  {
    log.info("testOffer");
    FixedBucketsHistogram h = buildHistogram(
        0,
        1000,
        1000,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{}
    );

    Random rng = ThreadLocalRandom.current();
    double[] values = new double[100000];
    for (int i = 0; i < 100000; i++) {
      values[i] = (double) rng.nextInt(1000);
      h.add(values[i]);
    }
    Arrays.sort(values);

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 25.0f, 50.0f, 98f});

    Percentile percentile = new Percentile();
    percentile.setData(values);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P25: " + quantiles[1]);
    log.info("MY-P50: " + quantiles[2]);
    log.info("MY-P98: " + quantiles[3]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P25: " + percentile.evaluate(25));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferWithNegatives()
  {
    log.info("testOfferWithNegative");
    FixedBucketsHistogram h = buildHistogram(
        -100,
        100,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES2
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES2.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES2[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues3()
  {
    log.info("testOfferValues3");

    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES3
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES3.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES3[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues32()
  {
    log.info("testOfferValues3");

    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES3
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f, 120f});
    double[] doubles = new double[VALUES3.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES3[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues4()
  {
    log.info("testOfferValues4");

    FixedBucketsHistogram h = buildHistogram(
        -100,
        100,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES4
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES4.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES4[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues5()
  {
    log.info("testOfferValues5");

    FixedBucketsHistogram h = buildHistogram(
        0,
        10,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES5
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES5.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES5[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues6()
  {
    log.info("testOfferValues6");

    FixedBucketsHistogram h = buildHistogram(
        0,
        10,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES6
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES6.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES6[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues7()
  {
    log.info("testOfferValues7");

    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES7
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES7.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES7[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testMergeSameBuckets()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{3, 8, 9, 13}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{3, 1, 2, 2, 1}, h.getHistogram());
    Assert.assertEquals(9, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeNoOverlapRight()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        50,
        100,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{60, 70, 80, 90}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(4, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeNoOverlapLeft()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        -100,
        -50,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{-60, -70, -80, -90}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(4, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }


  @Test
  public void testMergeSameBucketsRightOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 2}, h.getHistogram());
    Assert.assertEquals(7, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeSameBucketsLeftOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(12, h.getLowerLimit(), 0.01);
    Assert.assertEquals(32, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(12, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(3, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeSameBucketsContainsOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(10, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(50, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0, 1, 2, 1, 1, 1, 0, 0, 1}, h.getHistogram());
    Assert.assertEquals(8, h.getCount());
    Assert.assertEquals(2, h.getMin(), 0.01);
    Assert.assertEquals(48, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeSameBucketsContainedByOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(4, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(10, h.getLowerLimit(), 0.01);
    Assert.assertEquals(30, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 2, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(11, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());
  }


  @Test
  public void testMergeSameBucketsContainedByOther2()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(4, h.getNumBuckets());
    Assert.assertEquals(7.5, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(30, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 1, 3, 1}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(0, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBuckets()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 18}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        7,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{3, 8, 9, 19}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 3, 1, 1, 2}, h.getHistogram());
    Assert.assertEquals(9, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(18, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsRightOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        12,
        32,
        7,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertEquals(7, h.getCount());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 2}, h.getHistogram());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsLeftOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        9,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(12, h.getLowerLimit(), 0.01);
    Assert.assertEquals(32, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(13, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(3, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsContainsOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        10,
        30,
        7,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 21, 29}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(10, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(50, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0, 2, 2, 1, 1, 1, 0, 0, 1}, h.getHistogram());
    Assert.assertEquals(9, h.getCount());
    Assert.assertEquals(2, h.getMin(), 0.01);
    Assert.assertEquals(48, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsContainedByOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        50,
        13,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(4, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(10, h.getLowerLimit(), 0.01);
    Assert.assertEquals(30, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 2, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(11, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());
  }

  @Test
  public void testSerde()
  {
    FixedBucketsHistogram h = buildHistogram(
        -10,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES3
    );

    h.add(300);
    h.add(400);
    h.add(500);
    h.add(-300);
    h.add(-700);
    h.incrementMissing();

    byte[] fullWithHeader = h.toBytesFull(true);
    byte[] lz4 = h.toBytesLZ4();
    String asBase64Full = h.toBase64(false);
    String asBase64LZ4 = h.toBase64(true);

    FixedBucketsHistogram fromFullWithHeader = FixedBucketsHistogram.fromBytes(fullWithHeader);
    Assert.assertEquals(h, fromFullWithHeader);

    FixedBucketsHistogram fromLZ4 = FixedBucketsHistogram.fromBytes(lz4);
    Assert.assertEquals(h, fromLZ4);

    FixedBucketsHistogram fromBase64Full = FixedBucketsHistogram.fromBase64(asBase64Full);
    Assert.assertEquals(h, fromBase64Full);

    FixedBucketsHistogram fromBase64LZ4 = FixedBucketsHistogram.fromBase64(asBase64LZ4);
    Assert.assertEquals(h, fromBase64LZ4);
  }
}
