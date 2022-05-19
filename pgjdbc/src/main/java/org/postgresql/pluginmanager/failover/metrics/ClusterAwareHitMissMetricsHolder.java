/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover.metrics;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterAwareHitMissMetricsHolder implements ClusterAwareMetricsReporter<Boolean> {

  protected String metricName;
  protected int numberOfReports;
  protected int numberOfHits;

  private final Object lockObject = new Object();

  /**
   * Initialize a metric holder with a metric name.
   *
   * @param metricName Metric name
   */
  public ClusterAwareHitMissMetricsHolder(String metricName) {
    this.metricName = metricName;
  }

  public void register(Boolean isHit) {
    synchronized (this.lockObject) {
      this.numberOfReports++;
      if (isHit) {
        this.numberOfHits++;
      }
    }
  }
  
  public void reportMetrics(Logger log) {
    StringBuilder logMessage = new StringBuilder(256);

    logMessage.append("** Performance Metrics Report for '");
    logMessage.append(this.metricName);
    logMessage.append("' **\n");
    logMessage.append("\nNumber of reports: ").append(this.numberOfReports);
    if (this.numberOfReports > 0) {
      logMessage.append("\nNumber of hits: ").append(this.numberOfHits);
      logMessage.append("\nRatio: ")
          .append(this.numberOfHits * 100.0 / this.numberOfReports)
          .append(" %");
    }

    log.log(Level.INFO, logMessage.toString());
  }
}
