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
package com.facebook.giraph.hive.input.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class HadoopNative {
    private static boolean loaded = false;
    private static Throwable error = null;

    public static synchronized void requireHadoopNative()
    {
        if (loaded) {
            return;
        }
        if (error != null) {
            throw new RuntimeException("failed to load Hadoop native library", error);
        }
        try {
            loadLibrary("hadoop");
            Field field = NativeCodeLoader.class.getDeclaredField("nativeCodeLoaded");
            field.setAccessible(true);
            field.set(null, true);

            // Use reflection to HACK fix caching bug in CodecPool
            // This hack works from a concurrency perspective in this codebase
            // because we assume that all threads that will access the CodecPool will
            // have already been synchronized by calling requireHadoopNative() at some point.
            field = CodecPool.class.getDeclaredField("COMPRESSOR_POOL");
            setFinalStatic(field, new HackListMap());
            field = CodecPool.class.getDeclaredField("DECOMPRESSOR_POOL");
            setFinalStatic(field, new HackListMap());

            CompressionCodecFactory.getCodecClasses(new Configuration());

            loaded = true;
        }
        catch (Throwable t) {
            error = t;
            throw new RuntimeException("failed to load Hadoop native library", error);
        }
    }

    private static void setFinalStatic(Field field, Object newValue)
            throws Exception
    {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    private static void loadLibrary(String name)
            throws IOException
    {
        URL url = Resources.getResource(HadoopNative.class, getLibraryPath(name));
        File file = File.createTempFile(name, null);
        file.deleteOnExit();
        Files.copy(Resources.newInputStreamSupplier(url), file);
        System.load(file.getAbsolutePath());
    }

    private static String getLibraryPath(String name)
    {
        return "/nativelib/" + getPlatform() + "/" + System.mapLibraryName(name);
    }

    private static String getPlatform()
    {
        String name = System.getProperty("os.name");
        String arch = System.getProperty("os.arch");
        return (name + "-" + arch).replace(' ', '_');
    }

    private static class HackListMap<K, V>
            extends AbstractMap<K, List<? extends V>>
    {
        @Override
        public Set<Entry<K, List<? extends V>>> entrySet()
        {
            return Collections.emptySet();
        }

        @Override
        public List<? extends V> put(K key, List<? extends V> value)
        {
            return null;
        }

        @Override
        public List<? extends V> get(Object key)
        {
            return Lists.newArrayList();
        }
    }
}
