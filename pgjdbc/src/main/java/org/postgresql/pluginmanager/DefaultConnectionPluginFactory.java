/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager;

import java.util.Properties;

/**
 * Initialize a {@link DefaultConnectionPlugin}.
 */
public final class DefaultConnectionPluginFactory implements IConnectionPluginFactory {
  @Override
  public IConnectionPlugin getInstance(
      ICurrentConnectionProvider currentConnectionProvider,
      Properties props) {
    return new DefaultConnectionPlugin(currentConnectionProvider);
  }
}
