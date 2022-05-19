/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import org.postgresql.util.HostSpec;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;
import java.util.List;

/**
 * Interface for Reader Failover Process handler. This handler implements all necessary logic to try
 * to reconnect to another reader host.
 */
public interface ReaderFailoverHandler {

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts       Cluster current topology.
   * @param currentHost The currently connected host that has failed.
   *
   * @return {@link ReaderFailoverResult} The results of this process.
   * @throws SQLException if an error occurs
   */
  ReaderFailoverResult failover(List<HostSpec> hosts, @Nullable HostSpec currentHost) throws SQLException;

  /**
   * Called to get any available reader connection. If no reader is available then result of process
   * is unsuccessful. This process will not attempt to connect to the writer host.
   *
   * @param hostList Cluster current topology.
   *
   * @return {@link ReaderFailoverResult} The results of this process.
   * @throws SQLException if an error occurs
   */
  ReaderFailoverResult getReaderConnection(List<HostSpec> hostList) throws SQLException;
}
