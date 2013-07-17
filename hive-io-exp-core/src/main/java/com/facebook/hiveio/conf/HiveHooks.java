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
package com.facebook.hiveio.conf;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContextImpl;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for using Hive Hooks
 */
public class HiveHooks {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveHooks.class);

  /** Constructor */
  private HiveHooks() { }

  /**
   * Returns the hooks specified in a configuration variable.  The hooks are
   * returned in a list in the order they were specified in the
   * configuration variable.
   *
   * @param <T>         Hook class type
   * @param conf        The Hive configuration
   * @param hookConfVar The configuration variable specifying a comma separated
   *                    list of the hook class names.
   * @param clazz       The super type of the hooks.
   * @return            A list of the hooks cast as the type specified in clazz,
   *                    in the order they are listed in the value of hookConfVar
   * @throws Exception
   */
  public static  <T extends Hook>
  List<T> getHooks(HiveConf conf, HiveConf.ConfVars hookConfVar,
      Class<T> clazz) throws Exception
  {
    List<T> hooks = new ArrayList<T>();
    String csHooks = conf.getVar(hookConfVar);
    if (csHooks == null) {
      return hooks;
    }

    csHooks = csHooks.trim();
    if (csHooks.equals("")) {
      return hooks;
    }

    String[] hookClasses = csHooks.split(",");

    for (String hookClass : hookClasses) {
      try {
        T hook = (T) Class.forName(hookClass.trim(), true,
            JavaUtils.getClassLoader()).newInstance();
        hooks.add(hook);
      } catch (ClassNotFoundException e) {
        LOG.error("getHooks: " + hookConfVar.varname +
            " Class not found:" + e.getMessage());
        throw e;
      }
    }

    return hooks;
  }

  /**
   * Run Hive pre-hooks
   *
   * @param hiveConf HiveConf
   */
  public static void runDriverPreHooks(HiveConf hiveConf)
  {
    try {
      runDriverPreHooksThrows(hiveConf);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.error("runPreHooks: Failed to get hooks", e);
    }
  }

  /**
   * Run Hive pre-hooks
   *
   * @param hiveConf HiveConf
   */
  public static void runDriverPreHooksThrows(HiveConf hiveConf)
    throws Exception
  {
    List<HiveDriverRunHook> hooks =
        getHooks(hiveConf, HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS,
            HiveDriverRunHook.class);
    HiveDriverRunHookContext context =
        new HiveDriverRunHookContextImpl(hiveConf, "no_cmd");
    for (HiveDriverRunHook hook : hooks) {
      LOG.info("runPreHooksThrows: Running hook " + hook + " of class " +
          hook.getClass());
      hook.preDriverRun(context);
    }
  }
}
