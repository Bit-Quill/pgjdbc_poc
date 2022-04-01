/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginManager.simple;

import org.postgresql.pluginManager.IConnectionPlugin;
import org.postgresql.pluginManager.IConnectionPluginFactory;
import org.postgresql.pluginManager.ICurrentConnectionProvider;

import java.util.Properties;

/**
 * Class initializing a {@link ExecutionTimeConnectionPlugin}.
 */
public class DummyConnectionPluginFactory implements IConnectionPluginFactory {
  @Override
  public IConnectionPlugin getInstance(
      ICurrentConnectionProvider currentConnectionProvider,
      Properties props) {
    return new DummyConnectionPlugin();
  }
}

