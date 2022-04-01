/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.benchmark.pluginManager;

import org.postgresql.util.ConnectionUtil;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Tests the time and memory required to create a connection. Note: due to TCP socket's turning into
 * TIME_WAIT state on close, it is rather hard to test lots of connection creations, so only 50
 * iterations are performed.
 *
 * <p>To run this and other benchmarks (you can run the class from within IDE):
 *
 * <blockquote> <code>mvn package &amp;&amp; java -classpath postgresql-driver.jar:target/benchmarks.jar
 * -Duser=postgres -Dpassword=postgres -Dport=5433  -wi 10 -i 10 -f 1</code> </blockquote>
 *
 * <p>To run with profiling:
 *
 * <blockquote> <code>java -classpath postgresql-driver.jar:target/benchmarks.jar -prof gc -f 1 -wi
 * 10 -i 10</code> </blockquote>
 */
@Fork(1)
@Measurement(iterations = 50)
@Warmup(iterations = 10)
@State(Scope.Thread)
@Threads(1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FinalizeConnection {
  private Properties connectionProperties_no_pm;
  private Properties connectionProperties_pm_0;
  private Properties connectionProperties_pm_1;
  private Properties connectionProperties_pm_10;
  private String connectionUrl;
  private Driver driver;

  @Setup(Level.Trial)
  public void setUp() throws SQLException {
    // Plugin Manager is disabled
    connectionProperties_no_pm = ConnectionUtil.getProperties();
    connectionProperties_no_pm.setProperty("useConnectionPlugins", "false");

    // Plugin Manager is enabled
    // No user plugins are loaded
    connectionProperties_pm_0 = ConnectionUtil.getProperties();
    connectionProperties_pm_0.setProperty("useConnectionPlugins", "true");
    connectionProperties_pm_0.setProperty("connectionPluginFactories", ""); // explicitly sets no plugin

    // Plugin Manager is enabled
    // A single dummy user plugin is loaded
    connectionProperties_pm_1 = ConnectionUtil.getProperties();
    connectionProperties_pm_1.setProperty("useConnectionPlugins", "true");
    connectionProperties_pm_1.setProperty("connectionPluginFactories", "org.postgresql.plugins.demo.DummyConnectionPluginFactory");

    // Plugin Manager is enabled
    // 10 dummy user plugins are loaded
    connectionProperties_pm_10 = ConnectionUtil.getProperties();
    connectionProperties_pm_10.setProperty("useConnectionPlugins", "true");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("org.postgresql.plugins.demo.DummyConnectionPluginFactory");
    }
    connectionProperties_pm_10.setProperty("connectionPluginFactories", sb.toString());

    connectionUrl = ConnectionUtil.getURL();
    driver = DriverManager.getDriver(connectionUrl);
  }

  @Benchmark
  public void baseline() throws SQLException {
  }

  @Benchmark
  public Connection createAndClose_disabled() throws SQLException {
    Connection connection = driver.connect(connectionUrl, connectionProperties_no_pm);
    connection.close();
    return connection;
  }

  @Benchmark
  public Connection createAndClose_0() throws SQLException {
    Connection connection = driver.connect(connectionUrl, connectionProperties_pm_0);
    connection.close();
    return connection;
  }

  @Benchmark
  public Connection createAndClose_1() throws SQLException {
    Connection connection = driver.connect(connectionUrl, connectionProperties_pm_1);
    connection.close();
    return connection;
  }

  @Benchmark
  public Connection createAndClose_10() throws SQLException {
    Connection connection = driver.connect(connectionUrl, connectionProperties_pm_10);
    connection.close();
    return connection;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(org.postgresql.benchmark.connection.FinalizeConnection.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}
