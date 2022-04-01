/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.benchmark.pluginManager;

import org.postgresql.benchmark.profilers.FlightRecorderProfiler;
import org.postgresql.util.ConnectionUtil;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Fork(value = 5, jvmArgsPrepend = "-Xmx128m")
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BindArray {
  private Connection connection_no_pm;
  private Connection connection_pm_0;
  private Connection connection_pm_1;
  private Connection connection_pm_10;

  private PreparedStatement ps_no_pm;
  private PreparedStatement ps_pm_0;
  private PreparedStatement ps_pm_1;
  private PreparedStatement ps_pm_10;

  Integer[] ints;

  int arraySize = 1000;

  @Setup(Level.Trial)
  public void setUp() throws SQLException {
    Properties props = ConnectionUtil.getProperties();
    props.setProperty("useConnectionPlugins", "false");
    connection_no_pm = DriverManager.getConnection(ConnectionUtil.getURL(), props);
    ps_no_pm = connection_no_pm.prepareStatement("SELECT ?");
    ints = new Integer[arraySize];
    for (int i = 0; i < arraySize; i++) {
      ints[i] = i + 1;
    }

    props = ConnectionUtil.getProperties();
    props.setProperty("useConnectionPlugins", "true");
    props.setProperty("connectionPluginFactories", ""); // explicitly sets no plugin
    connection_pm_0 = DriverManager.getConnection(ConnectionUtil.getURL(), props);
    ps_pm_0 = connection_pm_0.prepareStatement("SELECT ?");
    ints = new Integer[arraySize];
    for (int i = 0; i < arraySize; i++) {
      ints[i] = i + 1;
    }

    props = ConnectionUtil.getProperties();
    props.setProperty("useConnectionPlugins", "true");
    props.setProperty("connectionPluginFactories", "org.postgresql.pluginManager.simple.DummyConnectionPluginFactory");
    connection_pm_1 = DriverManager.getConnection(ConnectionUtil.getURL(), props);
    ps_pm_1 = connection_pm_1.prepareStatement("SELECT ?");
    ints = new Integer[arraySize];
    for (int i = 0; i < arraySize; i++) {
      ints[i] = i + 1;
    }

    props = ConnectionUtil.getProperties();
    props.setProperty("useConnectionPlugins", "true");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("org.postgresql.pluginManager.simple.DummyConnectionPluginFactory");
    }
    props.setProperty("connectionPluginFactories", sb.toString());
    connection_pm_10 = DriverManager.getConnection(ConnectionUtil.getURL(), props);
    ps_pm_10 = connection_pm_10.prepareStatement("SELECT ?");
    ints = new Integer[arraySize];
    for (int i = 0; i < arraySize; i++) {
      ints[i] = i + 1;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws SQLException {
    ps_no_pm.close();
    connection_no_pm.close();

    ps_pm_0.close();
    connection_pm_0.close();

    ps_pm_1.close();
    connection_pm_1.close();

    ps_pm_10.close();
    connection_pm_10.close();
  }

  @Benchmark
  public Statement setObject_disabled() throws SQLException {
    Array sqlInts = connection_no_pm.createArrayOf("int", ints);
    ps_no_pm.setObject(1, sqlInts, Types.ARRAY);
    return ps_no_pm;
  }

  @Benchmark
  public Statement setObject_0() throws SQLException {
    Array sqlInts = connection_pm_0.createArrayOf("int", ints);
    ps_pm_0.setObject(1, sqlInts, Types.ARRAY);
    return ps_pm_0;
  }

  @Benchmark
  public Statement setObject_1() throws SQLException {
    Array sqlInts = connection_pm_1.createArrayOf("int", ints);
    ps_pm_1.setObject(1, sqlInts, Types.ARRAY);
    return ps_pm_1;
  }

  @Benchmark
  public Statement setObject_10() throws SQLException {
    Array sqlInts = connection_pm_10.createArrayOf("int", ints);
    ps_pm_10.setObject(1, sqlInts, Types.ARRAY);
    return ps_pm_10;
  }

  @Benchmark
  public Statement setArray_disabled() throws SQLException {
    Array sqlInts = connection_no_pm.createArrayOf("int", ints);
    ps_no_pm.setArray(1, sqlInts);
    return ps_no_pm;
  }

  @Benchmark
  public Statement setArray_0() throws SQLException {
    Array sqlInts = connection_pm_0.createArrayOf("int", ints);
    ps_pm_0.setArray(1, sqlInts);
    return ps_pm_0;
  }

  @Benchmark
  public Statement setArray_1() throws SQLException {
    Array sqlInts = connection_pm_1.createArrayOf("int", ints);
    ps_pm_1.setArray(1, sqlInts);
    return ps_pm_1;
  }

  @Benchmark
  public Statement setArray_10() throws SQLException {
    Array sqlInts = connection_pm_10.createArrayOf("int", ints);
    ps_pm_10.setArray(1, sqlInts);
    return ps_pm_10;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(org.postgresql.benchmark.statement.BindArray.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .addProfiler(FlightRecorderProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }
}
