/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginManager;

import java.util.Properties;

/**
 * Interface for connection plugin factories. This class implements ways to initialize a
 * connection plugin.
 */
public interface IConnectionPluginFactory {
  IConnectionPlugin getInstance(ICurrentConnectionProvider currentConnectionProvider, Properties props);
}
