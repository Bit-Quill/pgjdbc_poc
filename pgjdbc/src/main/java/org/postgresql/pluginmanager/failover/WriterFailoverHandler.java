/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import org.postgresql.util.HostSpec;

import java.sql.SQLException;
import java.util.List;

/**
 * Interface for Writer Failover Process handler. This handler implements all necessary logic to try
 * to reconnect to a current writer host or to a newly elected writer.
 */
public interface WriterFailoverHandler {

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link WriterFailoverResult} The results of this process.
   * @throws SQLException if failover fails
   */
  WriterFailoverResult failover(List<HostSpec> currentTopology) throws SQLException;
}
