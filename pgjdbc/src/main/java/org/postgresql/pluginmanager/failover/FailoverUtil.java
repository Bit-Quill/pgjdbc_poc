/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import org.postgresql.util.HostSpec;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Properties;

public class FailoverUtil {
  private static final String HOST_PORT_SEPARATOR = ":";

  static String getHostPortPair(final HostSpec hostSpec) {
    return hostSpec.getHost() + HOST_PORT_SEPARATOR + hostSpec.getPort();
  }

  /**
   * Accessor method to retrieve the url of the host connection
   *
   * @param dbname The database name used in the connection
   * @return A string that contains the connection url
   */
  public static @Nullable String getUrl(HostSpec hostSpec, @Nullable String dbname) {
    return getUrlFromEndpoint(hostSpec.getHost(), hostSpec.getPort(), dbname);
  }

  /**
   * Accessor method to retrieve the url of the host connection
   *
   * @return A string that contains the connection url
   */
  public static @Nullable String getUrl(HostSpec hostSpec) {
    return getUrl(hostSpec, "");
  }

  /**
   * Accessor method to retrieve the url of the host connection
   *
   * @param props The Properties to use
   * @return A string that contains the connection url
   */
  public static @Nullable String getUrl(HostSpec hostSpec, @Nullable Properties props) {
    String dbname = props == null ? "" : props.getProperty("PGDBNAME", "");
    return getUrl(hostSpec, dbname);
  }

  /**
   * Creates a connection string URL from an endpoint, port, and the database name
   *
   * @param endpoint The endpoint used in the connection
   * @param port     The port number used in the connection
   * @param dbname   The database name used in the connection
   * @return a connection string to an instance
   */
  private static String getUrlFromEndpoint(String endpoint, int port, @Nullable String dbname) {
    if (dbname == null) {
      dbname = "";
    }
    return String.format("//%s:%d/%s", endpoint, port, dbname);
  }
}
