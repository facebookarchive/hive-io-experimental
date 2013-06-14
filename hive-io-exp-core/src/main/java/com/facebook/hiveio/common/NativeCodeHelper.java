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

import com.google.common.io.Files;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Helper for loading native libraries
 */
public class NativeCodeHelper {
  /** Don't construct */
  protected NativeCodeHelper() { }

  /**
   * Load a library
   *
   * @param name String name of library
   * @throws IOException
   */
  protected static void loadLibrary(String name) throws IOException {
    URL url = Resources.getResource(HadoopNative.class, getLibraryPath(name));
    File file = File.createTempFile(name, null);
    file.deleteOnExit();
    Files.copy(Resources.newInputStreamSupplier(url), file);
    System.load(file.getAbsolutePath());
  }

  /**
   * Get platform specific path to library
   *
   * @param name String name of library
   * @return platform specific path to library
   */
  protected static String getLibraryPath(String name) {
    return "/nativelib/" + getPlatform() + "/" + System.mapLibraryName(name);
  }

  /**
   * Get platform we're on
   *
   * @return platform string
   */
  protected static String getPlatform() {
    String name = System.getProperty("os.name");
    String arch = System.getProperty("os.arch");
    return (name + "-" + arch).replace(' ', '_');
  }
}
