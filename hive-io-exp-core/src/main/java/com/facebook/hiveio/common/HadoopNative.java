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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Helper for loading Hadoop native libraries
 */
public class HadoopNative extends NativeCodeHelper {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HadoopNative.class);

  /** Have we loaded the library already */
  private static boolean LOADED = false;
  /** Did the loading cause an error */
  private static Throwable ERROR = null;

  /** Don't construct */
  private HadoopNative() { }

  /** Load native libraries */
  public static synchronized void requireHadoopNative() {
    if (LOADED) {
      return;
    }
    if (ERROR != null) {
      throw new RuntimeException("failed to load Hadoop native library", ERROR);
    }
    try {
      loadLibrary("hadoop");
      Field field = NativeCodeLoader.class.getDeclaredField("nativeCodeLoaded");
      field.setAccessible(true);
      field.set(null, true);

      // Use reflection to HACK fix caching bug in CodecPool. This hack works
      // from a concurrency perspective in this codebase because we assume that
      // all threads that will access the CodecPool will have already been
      // synchronized by calling requireHadoopNative() at some point.
      field = CodecPool.class.getDeclaredField("COMPRESSOR_POOL");
      setFinalStatic(field, new HackListMap());
      field = CodecPool.class.getDeclaredField("DECOMPRESSOR_POOL");
      setFinalStatic(field, new HackListMap());

      List<Class<? extends CompressionCodec>> codecs =
          CompressionCodecFactory.getCodecClasses(new Configuration());
      LOG.info("Compression Codecs: {}", codecs);

      LOADED = true;
      // CHECKSTYLE: stop IllegalCatch
    } catch (Throwable t) {
      // CHECKSTYLE: resume IllegalCatch
      ERROR = t;
      throw new RuntimeException("failed to load Hadoop native library", ERROR);
    }
  }

  /**
   * Set value of a final static field
   *
   * @param field Field to alter
   * @param newValue Value to set to
   * @throws Exception
   */
  private static void setFinalStatic(Field field, Object newValue)
    throws Exception
  {
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    field.set(null, newValue);
  }

  /**
   * Hack for map with list values
   *
   * @param <K> key type
   * @param <V> value type
   */
  private static class HackListMap<K, V>
      extends AbstractMap<K, List<? extends V>> {
    @Override
    public Set<Entry<K, List<? extends V>>> entrySet() {
      return Collections.emptySet();
    }

    @Override
    public List<? extends V> put(K key, List<? extends V> value) {
      return null;
    }

    @Override
    public List<? extends V> get(Object key) {
      return Lists.newArrayList();
    }
  }
}
