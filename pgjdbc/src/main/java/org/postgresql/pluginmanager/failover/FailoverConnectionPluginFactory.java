/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import org.postgresql.pluginmanager.IConnectionPlugin;
import org.postgresql.pluginmanager.IConnectionPluginFactory;
import org.postgresql.pluginmanager.ICurrentConnectionProvider;

import java.util.Properties;

public class FailoverConnectionPluginFactory implements IConnectionPluginFactory {
  @Override
  public IConnectionPlugin getInstance(
      ICurrentConnectionProvider currentConnectionProvider,
      Properties props) {
    return new FailoverConnectionPlugin(currentConnectionProvider, props);
  }
}
