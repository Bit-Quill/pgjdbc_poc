/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginManager;

import java.sql.Connection;

import org.postgresql.core.BaseConnection;
import org.postgresql.util.HostSpec;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface ICurrentConnectionProvider {
  BaseConnection getCurrentConnection();

  HostSpec getCurrentHostSpec();

  void setCurrentConnection(BaseConnection connection, HostSpec hostSpec);
}
