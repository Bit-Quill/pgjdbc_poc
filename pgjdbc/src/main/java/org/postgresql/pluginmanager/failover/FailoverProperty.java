/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public enum FailoverProperty {
  /**
   * Cluster Aware Failover Parameter: The unique identifier for a cluster
   */
  CLUSTER_ID(
      "clusterId",
      null,
      "Unique identifier for the cluster. Connections with the same cluster id share a"
          + " cluster topology"),

  /**
   * Cluster Aware Failover Parameter: The DNS pattern to build an instance endpoint
   */
  CLUSTER_INSTANCE_HOST_PATTERN(
      "clusterInstanceHostPattern",
      null,
      "Cluster instance DNS pattern that will be used to build a complete instance endpoint"),

  /**
   * Cluster Aware Failover Parameter: The refresh rate of the cluster topology
   */
  CLUSTER_TOPOLOGY_REFRESH_RATE_MS(
      "clusterTopologyRefreshRateMs",
      "30000",
      "Cluster topology refresh rate in milliseconds."),

  /**
   * Cluster Aware Failover Parameter: Allows the enabling and disabling of cluster aware failover
   */
  ENABLE_CLUSTER_AWARE_FAILOVER(
      "enableClusterAwareFailover",
      "true",
      "Set to true to enable the fast failover behavior offerred by the AWS JDBC Driver."
          + " Set to false for simple JDBC connections that do not require fast failover functionality."),
  /**
   * Cluster Aware Failover Parameter: The topology refresh rate for the writer failover process
   */
  FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS(
      "failoverClusterTopologyRefreshRateMs",
      "5000",
      "Cluster topology refresh rate in milliseconds during a writer failover process."),

  /**
   * Cluster Aware Failover Parameter: The allowed time to connect to a reader instance during failover
   */
  FAILOVER_READER_CONNECT_TIMEOUT_MS(
      "failoverReaderConnectTimeoutMs",
      "5000",
      "Maximum allowed time in milliseconds to attempt to connect to a reader instance during a reader failover process"),

  /**
   * Cluster Aware Failover Parameter: The maximum time allowed to attempt reconnecting to a new writer or reader
   */
  FAILOVER_TIMEOUT_MS(
      "failoverTimeoutMs",
      "60000",
      "Maximum allowed time in milliseconds to attempt reconnecting to a new writer or reader instance."),

  /**
   * Cluster Aware Failover Parameter: The amount of time allowed to reconnect to a writer during failover
   */
  FAILOVER_WRITER_RECONNECT_INTERVAL_MS(
      "failoverWriterReconnectIntervalMs",
      "5000",
      "Interval of time in milliseconds to wait between attempts to reconnect to a failed writer during writer failover process"),

  /**
   * Cluster Aware Failover Parameter:
   */
  GATHER_PERF_METRICS(
      "gatherPerfMetrics",
      "false",
      "A parameter that enables or disables reporting performance metrics of the cluster aware failover functionality"
  );

  private final String name;
  private final @Nullable String defaultValue;
  private final boolean required;
  private final String description;
  private final String @Nullable [] choices;
  private final boolean deprecated;

  FailoverProperty(String name, @Nullable String defaultValue, String description) {
    this(name, defaultValue, description, false);
  }

  FailoverProperty(String name, @Nullable String defaultValue, String description, boolean required) {
    this(name, defaultValue, description, required, (String[]) null);
  }

  FailoverProperty(String name, @Nullable String defaultValue, String description, boolean required,
      String @Nullable [] choices) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.required = required;
    this.description = description;
    this.choices = choices;
    try {
      this.deprecated = FailoverProperty.class.getField(name()).getAnnotation(Deprecated.class) != null;
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Map<String, FailoverProperty> PROPS_BY_NAME = new HashMap<String, FailoverProperty>();
  static {
    for (FailoverProperty prop : FailoverProperty.values()) {
      if (PROPS_BY_NAME.put(prop.getName(), prop) != null) {
        throw new IllegalStateException("Duplicate FailoverProperty name: " + prop.getName());
      }
    }
  }

  /**
   * Returns the name of the connection parameter. The name is the key that must be used in JDBC URL
   * or in Driver properties
   *
   * @return the name of the connection parameter
   */
  public String getName() {
    return name;
  }


  /**
   * Returns the value of the connection parameters according to the given {@code Properties} or the
   * default value.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter
   */
  public @Nullable String get(Properties properties) {
    return properties.getProperty(name, defaultValue);
  }

  /**
   * Set the value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties in which the value should be set
   * @param value value for this connection parameter
   */
  public void set(Properties properties, @Nullable String value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.setProperty(name, value);
    }
  }

  /**
   * Return the boolean value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter converted to boolean
   */
  public boolean getBoolean(Properties properties) {
    return Boolean.parseBoolean(get(properties));
  }

  /**
   * Return the int value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter converted to int
   * @throws PSQLException if it cannot be converted to int.
   */
  @SuppressWarnings("nullness:argument.type.incompatible")
  public int getInt(Properties properties) throws PSQLException {
    String value = get(properties);
    try {
      //noinspection ConstantConditions
      return Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      throw new PSQLException(GT.tr("{0} parameter value must be an integer but was: {1}",
          getName(), value), PSQLState.INVALID_PARAMETER_VALUE, nfe);
    }
  }
}
