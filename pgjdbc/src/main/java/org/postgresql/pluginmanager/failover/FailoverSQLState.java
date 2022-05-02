/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

/**
 * This class is used for holding SQLState codes related to failover.
 */
public enum FailoverSQLState {
  /**
   * Method of communication changed (from reasons such as failover)
   */
  COMMUNICATION_LINK_CHANGED("08S02");

  private final String state;

  FailoverSQLState(String state) {
    this.state = state;
  }

  public String getState() {
    return this.state;
  }
}
