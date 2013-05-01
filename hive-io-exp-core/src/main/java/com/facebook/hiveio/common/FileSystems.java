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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.net.URI;

/**
 * Helpers for Hadopo FileSystems
 */
public class FileSystems {
  /** Filter for hidden files starting with '_' or '.' */
  public static final PathFilter HIDDEN_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /** Don't construct, allow inheritance */
  protected FileSystems() { }

  /**
   * List files in directory that are not hidden.
   *
   * @param fs FileSystem
   * @param dir directory to list
   * @return FileStatus[] of non-hidden entries
   * @throws IOException I/O problems
   */
  public static FileStatus[] listNonHidden(FileSystem fs, Path dir)
    throws IOException {
    return fs.globStatus(new Path(dir, "*"), HIDDEN_FILTER);
  }

  /**
   * Check if directory has any non-hidden files.
   *
   * @param fs FileSystem
   * @param dir directory to check
   * @return true if directory has no non-hidden files
   * @throws IOException I/O problems
   */
  public static boolean dirHasNonHiddenFiles(FileSystem fs, Path dir)
    throws IOException {
    return listNonHidden(fs, dir).length != 0;
  }

  /**
   * Get path to file from source directory in a target directory.
   * Computes relative path of file in source and adds that to destination.
   *
   * @param file path to file
   * @param srcDir path to source directory
   * @param destDir path to target directory
   * @return path of file in destination
   */
  public static Path pathInDestination(Path file, Path srcDir, Path destDir) {
    URI fileUri = file.toUri();
    URI relativeUri = srcDir.toUri().relativize(fileUri);
    if (relativeUri == fileUri) {
      throw new IllegalArgumentException("Could not get file " + file +
          " relative path in " + srcDir);
    }
    String relativePath = relativeUri.getPath();
    if (relativePath.isEmpty()) {
      return destDir;
    } else {
      return new Path(destDir, relativePath);
    }
  }

  /**
   * Move a file or directory from source to destination, recursively copying
   * subdirectories.
   *
   * @param fs FileSystem
   * @param file path to copy (file or directory)
   * @param src path to source directory
   * @param dest path to destination directory
   * @throws IOException I/O problems
   */
  public static void move(FileSystem fs, Path file, Path src, Path dest)
    throws IOException {
    Path destFilePath = pathInDestination(file, src, dest);
    if (fs.isFile(file)) {
      if (fs.exists(destFilePath)) {
        if (!fs.delete(destFilePath, true)) {
          throw new IllegalArgumentException("Could not remove existing file " +
              destFilePath);
        }
      }
      if (!fs.rename(file, destFilePath)) {
        throw new IllegalArgumentException("Could not move " + file + " to " +
            destFilePath);
      }
    } else if (fs.getFileStatus(file).isDir()) {
      FileStatus[] statuses = fs.listStatus(file);
      fs.mkdirs(destFilePath);
      if (statuses != null) {
        for (FileStatus status : statuses) {
          move(fs, status.getPath(), src, dest);
        }
      }
    }
  }
}
