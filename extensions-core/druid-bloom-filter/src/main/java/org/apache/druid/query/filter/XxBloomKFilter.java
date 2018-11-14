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

package org.apache.druid.query.filter;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.hive.common.util.Murmur3;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class XxBloomKFilter extends BloomKFilter
{
  // todo: use random to generate a long instead of copying a random long from murmur3 impl...
  long seed = 0x87c37b91114253d5L;
  private static XXHashFactory factory = XXHashFactory.fastestInstance();

  private final XXHash64 hash64;

  public XxBloomKFilter(long maxNumEntries)
  {
    super(maxNumEntries);
    hash64 = factory.hash64();
  }

  public XxBloomKFilter(long[] bits, int numFuncs)
  {
    super(bits, numFuncs);
    hash64 = factory.hash64();
  }


  @Override
  public void addBytes(byte[] val, int offset, int length)
  {
    long hash = val == null ? Murmur3.NULL_HASHCODE : hash64.hash(val, offset, length, seed);
    addHash(hash);
  }

  @Override
  public void addInt(int val)
  {
    // puts int in little endian order
    addBytes(intToByteArrayLE(val));
  }

  @Override
  public void addLong(long val)
  {
    addBytes(longToByteArrayLE(val));
  }

  @Override
  public boolean testBytes(byte[] val, int offset, int length)
  {
    // todo: is it legit to use murmur3 null hashcode here?
    long hash = val == null ? Murmur3.NULL_HASHCODE :
                hash64.hash(val, offset, length, seed);
    return testHash(hash);
  }

  @Override
  public boolean testLong(long val)
  {
    return testBytes(longToByteArrayLE(val));
  }

  public static void serialize(OutputStream out, XxBloomKFilter bloomFilter) throws IOException
  {
    DataOutputStream dataOutputStream = new DataOutputStream(out);
    dataOutputStream.writeByte(bloomFilter.k);
    dataOutputStream.writeInt(bloomFilter.getBitSet().length);
    for (long value : bloomFilter.getBitSet()) {
      dataOutputStream.writeLong(value);
    }
  }

  public static XxBloomKFilter deserialize(InputStream in) throws IOException
  {
    if (in == null) {
      throw new IOException("Input stream is null");
    }

    try {
      DataInputStream dataInputStream = new DataInputStream(in);
      int numHashFunc = dataInputStream.readByte();
      int bitsetArrayLen = dataInputStream.readInt();
      long[] data = new long[bitsetArrayLen];
      for (int i = 0; i < bitsetArrayLen; i++) {
        data[i] = dataInputStream.readLong();
      }
      return new XxBloomKFilter(data, numHashFunc);
    }
    catch (RuntimeException e) {
      IOException io = new IOException("Unable to deserialize BloomKFilter");
      io.initCause(e);
      throw io;
    }
  }
}
