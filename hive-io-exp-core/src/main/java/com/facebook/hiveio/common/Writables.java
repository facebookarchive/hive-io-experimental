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

package com.facebook.hiveio.common;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Helpers for dealing with Writables
 */
public class Writables {
  /** Don't construct */
  protected Writables() { }

  /**
   * Read a class
   *
   * @param in DataInput
   * @param <T> type of class
   * @return Class for type
   * @throws IOException I/O errors
   */
  public static <T> Class<T> readClass(DataInput in) throws IOException {
    String className = WritableUtils.readString(in);
    try {
      return (Class<T>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could now find class named " + className, e);
    }
  }

  /**
   * Write class name
   *
   * @param out DataOutput
   * @param klass Class
   * @throws IOException I/O errors
   */
  public static void writeClassName(DataOutput out, Class<?> klass)
    throws IOException {
    WritableUtils.writeString(out, klass.getName());
  }

  /**
   * Write class name of object
   *
   * @param out DataOutput
   * @param object object
   * @throws IOException I/O errors
   */
  public static void writeClassName(DataOutput out, Object object)
    throws IOException {
    writeClassName(out, object.getClass());
  }

  /**
   * Read an enum array
   *
   * @param in DataInput
   * @param klass enum class
   * @param <E> type of enum
   * @return array of enums
   * @throws IOException
   */
  public static <E extends Enum<E>> E[] readEnumArray(DataInput in, Class<E> klass)
    throws IOException
  {
    int length = in.readInt();
    if (length > 0) {
      String readClassName = WritableUtils.readString(in);
      Class<?> readClass = Classes.classForName(readClassName);
      checkArgument(klass.equals(readClass),
          "Expected Enum class %s, read %s", klass.getName(), readClassName);
    }
    E[] enums = (E[]) Array.newInstance(klass, length);
    for (int i = 0; i < length; ++i) {
      enums[i] = WritableUtils.readEnum(in, klass);
    }
    return enums;
  }

  /**
   * Write an array of enums
   *
   * @param enums Enum array
   * @param out DataOutput
   * @param <E> type of enum
   * @throws IOException
   */
  public static <E extends Enum<E>> void writeEnumArray(DataOutput out, Enum<E>[] enums)
    throws IOException
  {
    out.writeInt(enums.length);
    if (enums.length > 0) {
      WritableUtils.writeString(out, enums[0].getDeclaringClass().getName());
    }
    for (Enum<E> val : enums) {
      WritableUtils.writeEnum(out, val);
    }
  }

  /**
   * Read a new instance of a class. Reads class name and creates a new
   * instance using reflection.
   *
   * @param in DataInput
   * @param <T> type of class
   * @return new instance of type
   * @throws IOException I/O errors
   */
  public static <T> T readNewInstance(DataInput in) throws IOException {
    Class<T> klass = readClass(in);
    try {
      Constructor<T> constructor = klass.getDeclaredConstructor();
      constructor.setAccessible(true);
      return constructor.newInstance();
      // CHECKSTYLE: stop IllegalCatchCheck
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatchCheck
      throw new IOException("Failed to construct class " + klass.getName(), e);
    }
  }

  /**
   * Read Hive field schemas
   *
   * @param in DataInput
   * @param fieldSchemas list to write to
   * @throws IOException I/O errors
   */
  public static void readFieldSchemas(DataInput in,
    List<FieldSchema> fieldSchemas) throws IOException {
    int numColumns = in.readInt();
    fieldSchemas.clear();
    for (int i = 0; i < numColumns; ++i) {
      fieldSchemas.add(readFieldSchema(in));
    }
  }

  /**
   * Read a single field schema
   *
   * @param in DataInput
   * @return field schema read
   * @throws IOException I/O errors
   */
  public static FieldSchema readFieldSchema(DataInput in) throws IOException {
    FieldSchema fs = new FieldSchema();
    fs.setName(WritableUtils.readString(in));
    fs.setType(WritableUtils.readString(in));
    return fs;
  }

  /**
   * Read an arbitrary Writable
   *
   * @param dataInput DataInput
   * @param <T> Type of writable
   * @return new writable instance
   * @throws IOException I/O errors
   */
  public static <T extends Writable> T readUnknownWritable(DataInput dataInput)
    throws IOException {
    T writable = (T) readNewInstance(dataInput);
    writable.readFields(dataInput);
    return writable;
  }

  /**
   * Write an arbitrary unknown Writable. This first writes the class name of
   * the writable so we can read it in dynamically later. It is used together
   * with readUnknownWritable.
   *
   * @param out DataOutput
   * @param object Writable to write
   * @throws IOException I/O errors
   */
  public static void writeUnknownWritable(DataOutput out, Writable object)
    throws IOException {
    writeClassName(out, object);
    object.write(out);
  }

  /**
   * Read in a Map<String, String>
   *
   * @param in DataInput
   * @param map Map to read into
   * @throws IOException I/O errors
   */
  public static void readStrStrMap(DataInput in, Map<String, String> map)
    throws IOException {
    int size = in.readInt();
    map.clear();
    for (int i = 0; i < size; ++i) {
      String key = WritableUtils.readString(in);
      String value = WritableUtils.readString(in);
      map.put(key, value);
    }
  }

  /**
   * Write a Map<String, String>
   *
   * @param out DataOutput
   * @param map Map to write
   * @throws IOException I/O errors
   */
  public static void writeStrStrMap(DataOutput out, Map<String, String> map)
    throws IOException {
    out.writeInt(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      WritableUtils.writeString(out, entry.getKey());
      WritableUtils.writeString(out, entry.getValue());
    }
  }

  /**
   * Read data for a Map<String, Integer>
   *
   * @param in DataInput
   * @param data Map to read into
   * @throws IOException I/O errors
   */
  public static void readStrIntMap(DataInput in, Map<String, Integer> data)
    throws IOException {
    int size = in.readInt();
    data.clear();
    for (int i = 0; i < size; ++i) {
      String key = WritableUtils.readString(in);
      int value = in.readInt();
      data.put(key, value);
    }
  }

  /**
   * Write a Map<String, Integer>
   *
   * @param out DataOutput
   * @param map Map to write
   * @throws IOException I/O errors
   */
  public static void writeStrIntMap(DataOutput out, Map<String, Integer> map)
    throws IOException {
    out.writeInt(map.size());
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      WritableUtils.writeString(out, entry.getKey());
      out.writeInt(entry.getValue());
    }
  }

  /**
   * Write an array of integers
   * @param out DataOutput
   * @param data array of ints
   * @throws IOException I/O errors
   */
  public static void writeIntArray(DataOutput out, int[] data) throws IOException {
    out.writeInt(data.length);
    for (int x : data) {
      out.writeInt(x);
    }
  }

  /**
   * Read an array of integers
   * @param in DataInput
   * @return array of ints
   * @throws IOException I/O errors
   */
  public static int[] readIntArray(DataInput in) throws IOException {
    int size = in.readInt();
    int[] result = new int[size];
    for (int i = 0; i < size; ++i) {
      result[i] = in.readInt();
    }
    return result;
  }

  /**
   * Read a List<String>
   *
   * @param in DataInput
   * @param data List to read into
   * @throws IOException I/O errors
   */
  public static void readStringList(DataInput in, List<String> data)
    throws IOException {
    int size = in.readInt();
    data.clear();
    for (int i = 0; i < size; ++i) {
      data.add(WritableUtils.readString(in));
    }
  }

  /**
   * Write a List<String>
   *
   * @param out DataOutput
   * @param data list of strings
   * @throws IOException I/O errors
   */
  public static void writeStringList(DataOutput out, List<String> data)
    throws IOException {
    out.writeInt(data.size());
    for (String s : data) {
      WritableUtils.writeString(out, s);
    }
  }

  /**
   * Write a collection of Hive FieldSchemas
   *
   * @param out DataOutput
   * @param fieldSchemas FieldSchema
   * @throws IOException I/O errors
   */
  public static void writeFieldSchemas(DataOutput out,
    Collection<FieldSchema> fieldSchemas) throws IOException {
    out.writeInt(fieldSchemas.size());
    for (FieldSchema fs : fieldSchemas) {
      writeFieldSchema(out, fs);
    }
  }

  /**
   * Write a Hive FieldSchema to output
   *
   * @param out DataOutput
   * @param fs FieldSchema
   * @throws IOException I/O errors
   */
  public static void writeFieldSchema(DataOutput out, FieldSchema fs)
    throws IOException {
    WritableUtils.writeString(out, fs.getName());
    WritableUtils.writeString(out, fs.getType());
  }

  /**
   * Read fields from byteArray to a Writable object.
   *
   * @param bytes Byte array to find the fields in.
   * @param writableObject Object to fill in the fields.
   */
  public static void readFieldsFromByteArray(
    byte[] bytes, Writable writableObject) {
    DataInputStream is = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      writableObject.readFields(is);
    } catch (IOException e) {
      throw new IllegalStateException(
          "readFieldsFromByteArray: IOException", e);
    }
  }

  /**
   * Write object to a byte array.
   *
   * @param writableObject Object to write from.
   * @return Byte array with serialized object.
   */
  public static byte[] writeToByteArray(Writable writableObject) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(os);
    try {
      writableObject.write(output);
    } catch (IOException e) {
      throw new IllegalStateException("writeToByteArray: IOStateException", e);
    }
    return os.toByteArray();
  }

  /**
   * Encode bytes to a String
   *
   * @param bytes byte[]
   * @return encoded String
   */
  private static String encodeBytes(byte[] bytes) {
    StringBuilder strBuf = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      strBuf.append((char) (((bytes[i] >> 4) & 0xF) + ('a')));
      strBuf.append((char) (((bytes[i]) & 0xF) + ('a')));
    }
    return strBuf.toString();
  }

  /**
   * Decode bytes from a String
   *
   * @param str String to decode
   * @return decoded byte[]
   */
  private static byte[] decodeBytes(String str) {
    byte[] bytes = new byte[str.length() / 2];
    for (int i = 0; i < str.length(); i += 2) {
      char c = str.charAt(i);
      bytes[i / 2] = (byte) ((c - 'a') << 4);
      c = str.charAt(i + 1);
      bytes[i / 2] += c - 'a';
    }
    return bytes;
  }

  /**
   * Write Writable to an encoded string
   *
   * @param writable Writable to write
   * @return encoded string with data
   */
  public static String writeToEncodedStr(Writable writable) {
    return encodeBytes(writeToByteArray(writable));
  }

  /**
   * Read fields from an encoded string
   *
   * @param str encoded String with data
   * @param writable Writable to read fields
   */
  public static void readFieldsFromEncodedStr(String str, Writable writable) {
    readFieldsFromByteArray(decodeBytes(str), writable);
  }

  /**
   * Write Serializable object to byte array
   *
   * @param object Object to write
   * @return Byte array
   */
  public static byte[] writeObjectToByteArray(Serializable object) throws IOException {
    checkNotNull(object, "object is null");
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(object);
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Read Serializable object from byte array
   *
   * @param data Byte array to read the object from
   * @param <T> Object class
   * @return Object read
   */
  public static <T extends Serializable> T readObjectFromByteArray(byte[] data) throws IOException {
    checkNotNull(data, "data is null");
    ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
    try {
      return (T) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Error finding class for " + new String(data, "UTF-8"), e);
    }
  }

  /**
   * Write Serializable object to String
   *
   * @param object Object to write
   * @return String
   */
  public static String writeObjectToString(Serializable object) {
    checkNotNull(object, "object is null");
    try {
      return encodeBytes(writeObjectToByteArray(object));
    } catch (IOException e) {
      throw new IllegalStateException("writeObject: IOException", e);
    }
  }

  /**
   * Read Serializable object from String
   *
   * @param string String to read the object from
   * @param <T> Object class
   * @return Object read
   */
  public static <T extends Serializable> T readObjectFromString(String string) {
    checkNotNull(string, "string is null");
    try {
      return (T) readObjectFromByteArray(decodeBytes(string));
    } catch (IOException e) {
      throw new IllegalStateException("readObject: IOException", e);
    }
  }
}
