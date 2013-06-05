/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.hiveio.bean;

import com.facebook.hiveio.common.UnsafeHelper;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.base.Objects;
import sun.misc.Unsafe;

public abstract class UnsafeFieldCopier extends FieldCopier {
  private static final Unsafe UNSAFE = UnsafeHelper.getUnsafe();
  private long toObjectOffset;

  protected long getToObjectOffset() {
    return toObjectOffset;
  }

  void setToObjectOffset(long toObjectIndex) {
    this.toObjectOffset = toObjectIndex;
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("fromHiveIndex", getFromHiveIndex())
        .add("toObjectOffset", toObjectOffset)
        .add("copierClass", getClass())
        .toString();
  }

  public static UnsafeFieldCopier fromType(Class<?> type) {
    UnsafeFieldCopier fieldCopier;
    if (type.equals(boolean.class)) {
      fieldCopier = new BooleanFC();
    } else if (type.equals(byte.class)) {
      fieldCopier = new ByteFC();
    } else if (type.equals(short.class)) {
      fieldCopier = new ShortFC();
    } else if (type.equals(int.class)) {
      fieldCopier = new IntFC();
    } else if (type.equals(long.class)) {
      fieldCopier = new LongFC();
    } else if (type.equals(float.class)) {
      fieldCopier = new FloatFC();
    } else if (type.equals(double.class)) {
      fieldCopier = new DoubleFC();
    } else {
      fieldCopier = new ObjectFC();
    }
    return fieldCopier;
  }

  private static class BooleanFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putBoolean(toObject, getToObjectOffset(),
          fromRecord.getBoolean(getFromHiveIndex()));
    }
  }

  private static class ByteFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putByte(toObject, getToObjectOffset(),
          (byte) fromRecord.getLong(getFromHiveIndex()));
    }
  }

  private static class ShortFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putShort(toObject, getToObjectOffset(),
          (short) fromRecord.getLong(getFromHiveIndex()));
    }
  }

  private static class IntFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putInt(toObject, getToObjectOffset(),
          (int) fromRecord.getLong(getFromHiveIndex()));
    }
  }

  private static class LongFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putLong(toObject, getToObjectOffset(),
          fromRecord.getLong(getFromHiveIndex()));
    }
  }

  private static class FloatFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putFloat(toObject, getToObjectOffset(),
          (float) fromRecord.getDouble(getFromHiveIndex()));
    }
  }

  private static class DoubleFC extends UnsafeFieldCopier {
    @Override
    public void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putDouble(toObject, getToObjectOffset(),
          fromRecord.getDouble(getFromHiveIndex()));
    }
  }

  private static class ObjectFC extends UnsafeFieldCopier {
    @Override
    protected void setValue(HiveReadableRecord fromRecord, Object toObject) {
      UNSAFE.putObject(toObject, getToObjectOffset(),
          fromRecord.get(getFromHiveIndex()));
    }
  }
}
