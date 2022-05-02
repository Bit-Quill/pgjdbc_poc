/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import static org.postgresql.pluginmanager.failover.AuroraTopologyService.NO_PORT;

import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;
import org.postgresql.pluginmanager.BasicConnectionProvider;
import org.postgresql.pluginmanager.IConnectionPlugin;
import org.postgresql.pluginmanager.IConnectionProvider;
import org.postgresql.pluginmanager.ICurrentConnectionProvider;
import org.postgresql.pluginmanager.failover.metrics.ClusterAwareMetrics;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class FailoverConnectionPlugin implements IConnectionPlugin {
  static final String METHOD_COMMIT = "commit";
  static final String METHOD_EQUALS = "equals";
  static final String METHOD_ROLLBACK = "rollback";
  static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  protected static final int DEFAULT_SOCKET_TIMEOUT = 10;
  protected static final int DEFAULT_CONNECT_TIMEOUT = 30;
  protected static final int WRITER_CONNECTION_INDEX = 0; // writer host is always stored at index 0

  final ICurrentConnectionProvider currentConnectionProvider;
  private boolean inTransaction;
  protected boolean isClosed = false;
  protected boolean closedExplicitly = false;
  private Throwable lastExceptionDealtWith;
  boolean explicitlyReadOnly;
  protected WriterFailoverHandler writerFailoverHandler;
  protected ReaderFailoverHandler readerFailoverHandler;
  protected ITopologyService topologyService;
  protected List<HostSpec> hosts = new ArrayList<>();
  protected Properties initialConnectionProps;

  // Configuration settings
  protected boolean enableFailoverSetting = true;
  protected int clusterTopologyRefreshRateMsSetting;
  protected @Nullable String clusterIdSetting;
  protected @Nullable String clusterInstanceHostPatternSetting;
  protected boolean gatherPerfMetricsSetting;
  protected int failoverTimeoutMsSetting;
  protected int failoverClusterTopologyRefreshRateMsSetting;
  protected int failoverWriterReconnectIntervalMsSetting;
  protected int failoverReaderConnectTimeoutMsSetting;
  protected int failoverConnectTimeout; //sec
  protected int failoverSocketTimeout; //sec

  protected final ClusterAwareMetrics metrics = new ClusterAwareMetrics();
  private long invokeStartTimeMs;
  private long failoverStartTimeMs;

  HostSpec currentHost;
  private IConnectionProvider connectionProvider;
  private RdsDnsAnalyzer rdsDnsAnalyzer;
  private boolean isRdsProxy;
  private boolean isRds;
  private boolean isClusterTopologyAvailable;

  public FailoverConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      Properties props) {
    this(currentConnectionProvider, props, new BasicConnectionProvider());
  }

  public FailoverConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      Properties props,
      IConnectionProvider connectionProvider) {
    this.currentConnectionProvider = currentConnectionProvider;
    this.initialConnectionProps = (Properties) props.clone();
    this.connectionProvider = connectionProvider;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(
            "openInitialConnection",
            "PreparedStatement.getMetaData",
            "PreparedStatement.executeLargeUpdate",
            "PreparedStatement.executeBatch",
            "PreparedStatement.executeUpdate",
            "PreparedStatement.execute",
            "PreparedStatement.executeQuery",
            "CallableStatement.getMetaData",
            "CallableStatement.executeLargeUpdate",
            "CallableStatement.executeBatch",
            "CallableStatement.executeUpdate",
            "CallableStatement.execute",
            "CallableStatement.executeQuery",
            "Statement.executeLargeUpdate",
            "Statement.executeBatch",
            "Statement.executeUpdate",
            "Statement.execute",
            "Statement.executeQuery",
            "ResultSet.next"
        )));
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args) throws Exception {
    if (METHOD_EQUALS.equals(methodName) && args[0] != null) {
      return args[0].equals(this);
    }

    Object result = null;

    try {
      result = executeSqlFunc.call();
    } catch (IllegalStateException e) {
      dealWithIllegalStateException(e);
    } catch (SQLException e) {
      dealWithOriginalException(e);
    }

    performSpecialMethodHandlingIfRequired(methodName, args);
    return result;
  }

  protected void dealWithIllegalStateException(IllegalStateException e) throws Exception {
    dealWithOriginalException(e.getCause());
  }

  private void dealWithOriginalException(Throwable originalException) throws Exception {
    if (this.lastExceptionDealtWith != originalException
        && shouldExceptionTriggerConnectionSwitch(originalException)) {

      if (this.gatherPerfMetricsSetting) {
        long currentTimeMs = System.currentTimeMillis();
        this.metrics.registerFailureDetectionTime(currentTimeMs - this.invokeStartTimeMs);
        this.invokeStartTimeMs = 0;
        this.failoverStartTimeMs = currentTimeMs;
      }

      invalidateCurrentConnection();
      pickNewConnection();
      this.lastExceptionDealtWith = originalException;
    }

    if (originalException instanceof Error) {
      throw (Error) originalException;
    }
    throw (Exception) originalException;
  }

  /**
   * Checks whether the exception indicates that we should initiate failover to a new
   * connection
   *
   * @param t The exception to check
   * @return True if failover should be initiated
   */
  protected boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
    if (!isFailoverEnabled()) {
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState != null) {
      return PSQLState.isConnectionError(sqlState)
          || PSQLState.COMMUNICATION_ERROR.getState().equals(sqlState);
    }

    return false;
  }

  /**
   * Perform any additional required steps after a method invocation that has been made through
   * this proxy. These steps
   * are only required for the methods that are indicated below.
   *
   * @param methodName The name of the method being called
   * @param args       The argument parameters of the method that is being called
   * @throws SQLException if failover is invoked when calling
   *                      {@link #connectToWriterIfRequired(boolean, boolean)}
   */
  private void performSpecialMethodHandlingIfRequired(String methodName, @Nullable Object[] args)
      throws SQLException {
    if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
      if (args[0] != null) {
        boolean autoCommit = (boolean) args[0];
        this.inTransaction = !autoCommit;
      }
    }

    if (METHOD_COMMIT.equals(methodName) || METHOD_ROLLBACK.equals(methodName)) {
      this.inTransaction = false;
    }

    if (METHOD_SET_READ_ONLY.equals(methodName) && args != null && args.length > 0
        && args[0] != null) {
      boolean originalReadOnlyValue = this.explicitlyReadOnly;
      this.explicitlyReadOnly = (boolean) args[0];
      connectToWriterIfRequired(originalReadOnlyValue, this.explicitlyReadOnly);
    }
  }

  /**
   * Connect to the writer if setReadOnly(false) was called and we are currently connected to a
   * reader instance.
   *
   * @param originalReadOnlyValue the original read-only value, before setReadOnly was called
   * @param newReadOnlyValue      the new read-only value, as passed to the setReadOnly call
   * @throws SQLException if {@link #failover()} is invoked
   */
  private void connectToWriterIfRequired(boolean originalReadOnlyValue, boolean newReadOnlyValue)
      throws SQLException {
    if (!shouldReconnectToWriter(originalReadOnlyValue, newReadOnlyValue) || this.hosts.isEmpty()) {
      return;
    }

    try {
      connectToHost(this.hosts.get(WRITER_CONNECTION_INDEX));
    } catch (SQLException e) {
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      failover();
    }
  }

  /**
   * Checks if the connection needs to be switched to a writer connection when setReadOnly(false)
   * is called. This should
   * only be true the read only value was switched from true to false and we aren't connected to
   * a writer.
   *
   * @param originalReadOnlyValue the original read-only value, before setReadOnly was called
   * @param newReadOnlyValue      the new read-only value, as passed to the setReadOnly call
   * @return true if we should reconnect to the writer instance, according to the setReadOnly call
   */
  private boolean shouldReconnectToWriter(boolean originalReadOnlyValue, boolean newReadOnlyValue) {
    final BaseConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();

    return originalReadOnlyValue
        && !newReadOnlyValue
        && ((this.currentHost == null)
        || !this.currentHost.equals(
        this.topologyService.getTopology(currentConnection, false).get(WRITER_CONNECTION_INDEX)));
  }

  protected synchronized void pickNewConnection() throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      // LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connection was closed by the user");
      return;
    }

    if (isConnected() || this.hosts.isEmpty()) {
      failover();
      return;
    }

    if (shouldAttemptReaderConnection()) {
      failoverReader();
      return;
    }

    try {
      connectToHost(this.hosts.get(WRITER_CONNECTION_INDEX));
      if (this.explicitlyReadOnly) {
        topologyService.setLastUsedReaderHost(this.currentHost);
      }
    } catch (SQLException e) {
      failover();
    }
  }

  /**
   * Checks if we should attempt failover to a reader instance in {@link #pickNewConnection()}
   *
   * @return True if a reader exists and the connection is read-only
   */
  @RequiresNonNull({"this.hosts"})
  private boolean shouldAttemptReaderConnection() {
    return this.explicitlyReadOnly && clusterContainsReader();
  }

  /**
   * Connect to the host specified by the given {@link HostSpec} and set the resulting connection
   * as the underlying connection for this proxy
   *
   * @param host The {@link HostSpec} representing the host to connect to
   * @throws SQLException if an error occurs while connecting
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.connectionProvider"})
  private synchronized void connectToHost(HostSpec host) throws SQLException {
    try {
      BaseConnection connection = createConnectionForHost(host, this.initialConnectionProps);
      final BaseConnection currentConnection =
          this.currentConnectionProvider.getCurrentConnection();
      invalidateCurrentConnection();

      boolean readOnly;
      if (this.explicitlyReadOnly) {
        readOnly = true;
      } else if (currentConnection != null) {
        readOnly = currentConnection.isReadOnly();
      } else {
        readOnly = false;
      }
      syncSessionState(currentConnection, connection, readOnly);
      this.currentConnectionProvider.setCurrentConnection(
          connection,
          new HostSpec(host.getHost(), host.getPort()));
      this.currentHost = host;
      this.inTransaction = false;
    } catch (SQLException e) {
      // if (currentConnection != null && host != null) {
      //   logConnectionFailure(host, e);
      // }
      throw e;
    }
  }

  /**
   * Set the target connection read-only status according to the given parameter, and the
   * autocommit and transaction
   * isolation status according to the source connection state.
   *
   * @param source   the source connection that holds the desired state for the target connection
   * @param target   the target connection that should hold the desired state from the source
   *                 connection at the end of this
   *                 method call
   * @param readOnly The new read-only status
   * @throws SQLException if an error occurs while setting the target connection's state
   */
  protected void syncSessionState(
      @Nullable BaseConnection source,
      @Nullable BaseConnection target,
      boolean readOnly) throws SQLException {
    if (target != null) {
      target.setReadOnly(readOnly);
    }

    if (source == null || target == null) {
      return;
    }

    target.setAutoCommit(source.getAutoCommit());
    target.setTransactionIsolation(source.getTransactionIsolation());
  }

  /**
   * Creates a connection to the host specified by the given {@link HostSpec}
   *
   * @param hostSpec the {@link HostSpec} specifying the host to connect to
   * @param props    The properties that should be passed to the new connection
   * @return a connection to the given host
   * @throws SQLException if an error occurs while connecting to the given host
   */
  @RequiresNonNull({"this.connectionProvider"})
  protected synchronized @Nullable BaseConnection createConnectionForHost(HostSpec hostSpec,
      Properties props)
      throws SQLException {
    final BaseConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    String dbname = props.getProperty("PGDBNAME", "");
    if (RdsDnsAnalyzer.isNullOrEmpty(dbname)) {
      String currentDbName = currentConnection.getCatalog();
      if (!RdsDnsAnalyzer.isNullOrEmpty(currentDbName)) {
        dbname = currentDbName;
      }
    }

    return this.connectionProvider.connect(
        new HostSpec[]{hostSpec},
        props,
        FailoverUtil.getUrl(hostSpec, dbname));
  }

  /**
   * Initiates the reader failover procedure
   *
   * @throws SQLException if failover was unsuccessful
   */
  @RequiresNonNull({"this.topologyService", "this.initialConnectionProps",
      "this.readerFailoverHandler", "this.metrics", "this.writerFailoverHandler",
      "this.connectionProvider", "this.hosts"})
  protected void failoverReader()
      throws SQLException {
    ReaderFailoverResult result = readerFailoverHandler.failover(this.hosts, this.currentHost);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerReaderFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (!result.isConnected()) {
      processFailoverFailureAndThrowException("Unable to establish a read-only connection");
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    this.currentHost = result.getHost();
    this.currentConnectionProvider.setCurrentConnection(
        result.getConnection(),
        new HostSpec(this.currentHost.getHost(), this.currentHost.getPort()));

    updateTopologyAndConnectIfNeeded(true);
    topologyService.setLastUsedReaderHost(this.currentHost);
  }

  /**
   * Method that updates the topology when necessary and establishes a new connection if the
   * proxy is not connected or
   * the current host is not found in the new topology.
   *
   * @param forceUpdate a boolean used to force an update instead of the default behavior of
   *                    updating only when necessary
   * @throws SQLException if an error occurs fetching the topology or while conducting failover
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics",
      "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider",
      "this.hosts"})
  protected void updateTopologyAndConnectIfNeeded(boolean forceUpdate) throws SQLException {
    if (!isFailoverEnabled()) {
      return;
    }

    if (!isConnected()) {
      pickNewConnection();
      return;
    }

    List<HostSpec> latestTopology = this.topologyService.getTopology(
        this.currentConnectionProvider.getCurrentConnection(),
        forceUpdate);

    if (latestTopology.isEmpty()) {
      return;
    }

    this.hosts = latestTopology;
    if (this.currentHost == null) {
      // Most likely the currentHost is not established because the original connection URL
      // specified an IP address,
      // reader cluster endpoint, custom domain, or custom cluster; skip scanning the new
      // topology for the currentHost
      return;
    }

    updateCurrentHost(latestTopology);
  }

  /**
   * Search through the given topology for an instance matching the host details of this
   * .currentHost, and update
   * this.currentHost accordingly. If no match is found, switch to another connection
   *
   * @param latestTopology The topology to scan for a match to this.currentHost
   * @throws SQLException if a match for the current host wasn't found in the given topology and
   *                      the resulting attempt
   *                      to switch connections encounters an error
   */
  @RequiresNonNull({"this.connectionProvider", "this.topologyService", "this.readerFailoverHandler",
      "this.writerFailoverHandler", "this.initialConnectionProps", "this.hosts", "this.metrics"})
  private void updateCurrentHost(List<HostSpec> latestTopology) throws SQLException {
    HostSpec latestHost = null;
    for (HostSpec host : latestTopology) {
      if (host != null && host.equals(this.currentHost)) {
        latestHost = host;
        break;
      }
    }

    if (latestHost == null) {
      // current connection host isn't found in the latest topology
      // switch to another connection;
      this.currentHost = null;
      pickNewConnection();
    } else {
      // found the same node at different position in the topology
      // adjust current host only; connection is still valid
      this.currentHost = latestHost;
    }
  }

  /**
   * Initiates the failover procedure. This process tries to establish a new connection to an
   * instance in the topology.
   *
   * @throws SQLException upon successful failover to indicate that failover has occurred and
   *                      session state should be
   *                      reconfigured by the user. May also throw a SQLException if failover is
   *                      unsuccessful.
   */
  @RequiresNonNull({"this.topologyService", "this.initialConnectionProps",
      "this.readerFailoverHandler", "this.writerFailoverHandler", "this.metrics",
      "this.connectionProvider", "this.hosts"})
  protected synchronized void failover()
      throws SQLException {
    final BaseConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    this.inTransaction =
        currentConnection.getQueryExecutor().getTransactionState() != TransactionState.IDLE;

    if (shouldPerformWriterFailover()) {
      failoverWriter();
    } else {
      failoverReader();
    }

    if (this.inTransaction) {
      this.inTransaction = false;
      String transactionResolutionUnknownError =
          "Transaction resolution unknown. Please re-configure session state if "
              + "required and try restarting the transaction.";
      // LOGGER.log(Level.SEVERE, transactionResolutionUnknownError);
      throw new SQLException(transactionResolutionUnknownError,
          PSQLState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
    } else {
      String connectionChangedError =
          "The active SQL connection has changed due to a connection failure. Please re-configure"
              + " session state if required.";
      throw new SQLException(connectionChangedError,
          FailoverSQLState.COMMUNICATION_LINK_CHANGED.getState());
    }
  }

  /**
   * Checks if the driver should conduct a writer failover
   *
   * @return true if the connection is not explicitly read only
   */
  private boolean shouldPerformWriterFailover() {
    return !this.explicitlyReadOnly;
  }

  /**
   * Initiates the writer failover procedure
   *
   * @throws SQLException if failover was unsuccessful
   */
  @RequiresNonNull({"this.topologyService", "this.initialConnectionProps",
      "this.writerFailoverHandler", "this.metrics", "this.hosts"})
  protected void failoverWriter()
      throws SQLException {
    // LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Starting writer failover procedure.");
    WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.hosts);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerWriterFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (!failoverResult.isConnected()) {
      processFailoverFailureAndThrowException(
          "Unable to establish SQL connection to the writer instance");
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    if (!failoverResult.getTopology().isEmpty()) {
      this.hosts = failoverResult.getTopology();
    }

    final HostSpec hostSpec = this.hosts.get(WRITER_CONNECTION_INDEX);
    this.currentConnectionProvider.setCurrentConnection(
        failoverResult.getNewConnection(),
        new HostSpec(hostSpec.getHost(), hostSpec.getPort()));
  }

  /**
   * Attempt rollback if the current connection is in a transaction, then close the connection.
   * Rollback may not be
   * possible as the current connection might be broken or closed when this method is called.
   */
  private synchronized void invalidateCurrentConnection() {
    final BaseConnection conn = this.currentConnectionProvider.getCurrentConnection();

    this.inTransaction =
        conn.getQueryExecutor().getTransactionState() != TransactionState.IDLE;
    if (this.inTransaction) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        // ignore
      }
    }

    try {
      if (!conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }
  }

  /**
   * Register and log a failover failure, and throw an exception
   *
   * @param message The message that should be logged and included in the thrown exception
   * @throws SQLException representing the failover failure error. This will always be thrown
   *                      when this method is called
   */
  private void processFailoverFailureAndThrowException(String message) throws SQLException {
    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(false);
    }
    throw new SQLException(message, PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState());
  }

  @Override
  public void openInitialConnection(
      HostSpec[] hostSpecs,
      Properties props,
      String url,
      Callable<Void> openInitialConnectionFunc) throws Exception {
    openInitialConnection(hostSpecs, props, url,
        openInitialConnectionFunc,
        new BasicConnectionProvider(),
        () -> new AuroraTopologyService(this.clusterTopologyRefreshRateMsSetting),
        () -> new ClusterAwareReaderFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.initialConnectionProps,
            this.failoverTimeoutMsSetting,
            this.failoverReaderConnectTimeoutMsSetting),
        () -> new ClusterAwareWriterFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.initialConnectionProps,
            this.readerFailoverHandler,
            this.failoverTimeoutMsSetting,
            this.failoverClusterTopologyRefreshRateMsSetting,
            this.failoverWriterReconnectIntervalMsSetting),
        new RdsDnsAnalyzer()
    );
  }

  void openInitialConnection(
      HostSpec[] hostSpecs,
      Properties props,
      String url,
      Callable<Void> openInitialConnectionFunc,
      IConnectionProvider connectionProvider,
      Supplier<ITopologyService> topologyServiceSupplier,
      Supplier<ClusterAwareReaderFailoverHandler> readerFailoverHandlerSupplier,
      Supplier<ClusterAwareWriterFailoverHandler> writerFailoverHandlerSupplier,
      RdsDnsAnalyzer rdsDnsAnalyzer
  ) throws Exception {
    openInitialConnectionFunc.call();

    initSettings(props);

    this.initialConnectionProps = (Properties) props.clone();
    this.topologyService = topologyServiceSupplier.get();

    if (this.topologyService instanceof AuroraTopologyService) {
      ((AuroraTopologyService) this.topologyService).setPerformanceMetrics(metrics,
          this.gatherPerfMetricsSetting);
    }

    this.connectionProvider = connectionProvider;
    this.readerFailoverHandler = readerFailoverHandlerSupplier.get();
    this.writerFailoverHandler = writerFailoverHandlerSupplier.get();
    this.rdsDnsAnalyzer = rdsDnsAnalyzer;

    final HostSpec hostSpec = hostSpecs[0];
    if (!this.enableFailoverSetting) {
      this.currentConnectionProvider.setCurrentConnection(
          this.connectionProvider.connect(hostSpecs, props, url),
          hostSpec
      );
      return;
    }

    if (!RdsDnsAnalyzer.isNullOrEmpty(this.clusterInstanceHostPatternSetting)) {
      initFromHostPatternSetting(hostSpec, props, url);
    } else if (IpAddressUtils.isIPv4(hostSpec.getHost()) || IpAddressUtils.isIPv6(
        hostSpec.getHost())) {
      initExpectingNoTopology(hostSpec, props, url);
    } else {
      identifyRdsType(hostSpec.getHost());
      if (!this.isRds) {
        initExpectingNoTopology(hostSpec, props, url);
      } else {
        initFromConnectionString(hostSpec, props, url);
      }
    }
  }

  /**
   * Initializes the connection proxy using the user-configured host pattern setting
   *
   * @param connectionStringHostSpec The {@link HostSpec} info for the desired connection
   * @param props                    The {@link Properties} specifying the connection and
   *                                 failover configuration
   * @param url                      The URL to connect to
   * @throws SQLException if an error occurs while attempting to initialize the proxy connection
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler",
      "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps",
      "this.metrics", "this.hosts"})
  private void initFromHostPatternSetting(
      HostSpec connectionStringHostSpec,
      Properties props,
      String url) throws SQLException {
    final HostSpec patternSettingHostSpec = getHostSpecFromHostPatternSetting();
    final String instanceHostPattern = patternSettingHostSpec.getHost();
    final int instanceHostPort = patternSettingHostSpec.getPort() != NO_PORT
        ? patternSettingHostSpec.getPort() : connectionStringHostSpec.getPort();

    setClusterId(instanceHostPattern, instanceHostPort);
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(instanceHostPattern, instanceHostPort));
    createConnectionAndInitializeTopology(connectionStringHostSpec, props, url);
  }

  /**
   * Retrieves the host and port number from the user-configured host pattern setting
   *
   * @return A {@link HostSpec} object containing the host and port number
   * @throws SQLException if an invalid value was used for the host pattern setting
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private HostSpec getHostSpecFromHostPatternSetting() throws SQLException {
    HostSpec hostSpec = RdsDnsAnalyzer.parseUrl(this.clusterInstanceHostPatternSetting);
    if (hostSpec == null) {
      throw new SQLException(
          "Invalid value in 'clusterInstanceHostPattern' configuration setting - the value could "
              + "not be parsed");
    }
    validateHostPatternSetting(hostSpec);
    return hostSpec;
  }

  /**
   * Validate that the host pattern setting is usable and in the correct format
   *
   * @param hostSpec The {@link HostSpec} containing the info defined by the host pattern setting
   * @throws SQLException if an invalid value was used for the host pattern setting
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private void validateHostPatternSetting(HostSpec hostSpec) throws SQLException {
    String hostPattern = hostSpec.getHost();

    if (!isDnsPatternValid(hostPattern)) {
      String invalidDnsPatternError =
          "Invalid value set for the 'clusterInstanceHostPattern' configuration setting - "
              + "the host pattern must contain a '?' character as a placeholder for the DB "
              + "instance identifiers of the "
              + "instances in the cluster";
      throw new SQLException(invalidDnsPatternError);
    }

    identifyRdsType(hostPattern);
    if (this.isRdsProxy) {
      String rdsProxyInstancePatternError =
          "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration "
              + "setting.";
      throw new SQLException(rdsProxyInstancePatternError);
    }

    if (this.rdsDnsAnalyzer.isRdsCustomClusterDns(hostPattern)) {
      String rdsCustomClusterInstancePatternError =
          "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' "
              + "configuration setting";
      throw new SQLException(rdsCustomClusterInstancePatternError);
    }
  }

  /**
   * Checks if the DNS pattern is valid
   *
   * @param pattern The string to check
   * @return True if the pattern is valid
   */
  private boolean isDnsPatternValid(String pattern) {
    return pattern.contains("?");
  }

  /**
   * Set isRds and isRdsProxy according to the host string
   *
   * @param host The endpoint for this connection
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private void identifyRdsType(String host) {
    this.isRds = this.rdsDnsAnalyzer.isRdsDns(host);
    this.isRdsProxy = this.rdsDnsAnalyzer.isRdsProxyDns(host);
  }

  /**
   * Initializes the connection proxy expecting that the connection does not provide cluster
   * information. This occurs when the
   * connection is to an IP address, custom domain, or non-RDS instance. In the first two cases,
   * if cluster information does exist, the user should have set the clusterInstanceHostPattern
   * setting to enable use of the topology. An exception will be thrown if this method is called
   * but topology information was retrieved using the connection.
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props    The {@link Properties} specifying the connection and failover configuration
   * @param url      The URL to connect to
   * @throws SQLException if an error occurs while establishing the connection proxy, or if
   *                      topology information was
   *                      retrieved while establishing the connection
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler",
      "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps",
      "this.metrics", "this.hosts"})
  private void initExpectingNoTopology(
      HostSpec hostSpec,
      Properties props, String url) throws SQLException {
    setClusterId(hostSpec.getHost(), hostSpec.getPort());
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(hostSpec.getHost(), hostSpec.getPort()));
    createConnectionAndInitializeTopology(hostSpec, props, url);

    if (this.isClusterTopologyAvailable) {
      String instanceHostPatternRequiredError =
          "The 'clusterInstanceHostPattern' configuration property is required when an IP address"
              + " or custom domain "
              + "is used to connect to a cluster that provides topology information. If you would"
              + " instead like to connect "
              + "without failover functionality, set the 'enableClusterAwareFailover' "
              + "configuration property to false.";
      throw new SQLException(instanceHostPatternRequiredError);
    }
  }

  /**
   * Initializes the connection proxy using a connection URL
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props    The {@link Properties} specifying the connection and failover configuration
   * @param url      The URL to connect to
   * @throws SQLException If an error occurs while establishing the connection proxy
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler",
      "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps",
      "this.metrics", "this.hosts"})
  private void initFromConnectionString(
      HostSpec hostSpec,
      Properties props,
      String url) throws SQLException {
    String rdsInstanceHostPattern =
        this.rdsDnsAnalyzer.getRdsInstanceHostPattern(hostSpec.getHost());
    if (rdsInstanceHostPattern == null) {
      String unexpectedConnectionStringPattern =
          "The provided connection string does not appear to match an expected Aurora DNS pattern"
              + ". Please set the "
              + "'clusterInstanceHostPattern' configuration property to specify the host pattern "
              + "for the cluster "
              + "you are trying to connect to.";
      throw new SQLException(unexpectedConnectionStringPattern);
    }

    setClusterId(hostSpec.getHost(), hostSpec.getPort());
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(rdsInstanceHostPattern, hostSpec.getPort()));
    createConnectionAndInitializeTopology(hostSpec, props, url);
  }

  /**
   * Sets the clusterID in the topology service
   *
   * @param host The host that will be used for the connection, or the instanceHostPattern setting
   * @param port The port that will be used for the connection
   */
  @RequiresNonNull({"this.topologyService", "this.rdsDnsAnalyzer"})
  private void setClusterId(
      String host,
      int port) {
    if (!Util.isNullOrEmpty(this.clusterIdSetting)) {
      this.topologyService.setClusterId(this.clusterIdSetting);
    } else if (this.isRdsProxy) {
      // Each proxy is associated with a single cluster so it's safe to use the RDS Proxy Url as
      // the cluster ID
      this.topologyService.setClusterId(host + ":" + port);
    } else if (this.isRds) {
      // If it's a cluster endpoint or reader cluster endpoint, then let's use it as the cluster ID
      String clusterRdsHostUrl = this.rdsDnsAnalyzer.getRdsClusterHostUrl(host);
      if (!Util.isNullOrEmpty(clusterRdsHostUrl)) {
        this.topologyService.setClusterId(clusterRdsHostUrl + ":" + port);
      }
    }
  }

  /**
   * Creates an instance template that the topology service will use to form topology information
   * for each
   * instance in the cluster
   *
   * @param host The new host for the {@link HostSpec} object
   * @param port The port for the connection
   * @return A {@link HostSpec} class for cluster instance template
   */
  private HostSpec createClusterInstanceTemplate(String host, int port) {
    return new HostSpec(host, port);
  }

  /**
   * Creates a connection and establishes topology information for connections that provide it
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props    The {@link Properties} specifying the connection and failover configuration
   * @param url      The URL to connect to
   * @throws SQLException if an error occurs while establishing the connection proxy
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler",
      "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps",
      "this.metrics", "this.hosts"})
  private synchronized void createConnectionAndInitializeTopology(
      HostSpec hostSpec,
      Properties props,
      String url) throws SQLException {
    boolean connectedUsingCachedTopology = createInitialConnection(hostSpec, props, url);
    initTopology(hostSpec, connectedUsingCachedTopology);
    finalizeConnection(connectedUsingCachedTopology);
  }

  /**
   * Establish the initial connection according to the given details. When the given host is the
   * writer cluster or reader
   * cluster endpoint and cached topology information is available, the writer or reader instance
   * to connect to will
   * be chosen based on this cached topology and verified later in
   * {@link #validateInitialConnection(boolean)}
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props    The {@link Properties} for the connection
   * @param url      The URL to connect to in the case that the hostSpec host is not the writer
   *                 cluster or reader cluster or
   *                 cached topology information doesn't exist
   * @return true if the connection was established using cached topology; otherwise, false
   * @throws SQLException if a connection could not be established
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.rdsDnsAnalyzer",
      "this.initialConnectionProps"})
  private boolean createInitialConnection(
      HostSpec hostSpec,
      Properties props, String url) throws SQLException {
    String host = hostSpec.getHost();
    boolean connectedUsingCachedTopology = false;

    if (this.rdsDnsAnalyzer.isRdsClusterDns(host)) {
      this.explicitlyReadOnly = this.rdsDnsAnalyzer.isReaderClusterDns(host);

      try {
        attemptConnectionUsingCachedTopology();
        if ((this.currentConnectionProvider.getCurrentConnection() != null)
            && (this.currentHost != null)) {
          connectedUsingCachedTopology = true;
        }
      } catch (SQLException e) {
        // do nothing - attempt to connect directly will be made below
      }
    }

    if (!isConnected()) {
      // Either URL was not a cluster endpoint or cached topology did not exist - connect
      // directly to URL
      this.currentConnectionProvider.setCurrentConnection(
          this.connectionProvider.connect(new HostSpec[]{hostSpec}, props, url),
          hostSpec
      );
    }
    return connectedUsingCachedTopology;
  }

  /**
   * Checks if the connection proxied by this class is open and usable
   *
   * @return true if the connection is open and usable
   */
  synchronized boolean isConnected() {
    try {
      final BaseConnection currentConnection =
          this.currentConnectionProvider.getCurrentConnection();
      return currentConnection != null && !currentConnection.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Attempts a connection using cached topology information
   *
   * @throws SQLException if an error occurs while establishing the connection
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService",
      "this.connectionProvider"})
  private void attemptConnectionUsingCachedTopology() throws SQLException {
    @Nullable List<HostSpec> cachedHosts = topologyService.getCachedTopology();
    if (cachedHosts != null && !cachedHosts.isEmpty()) {
      this.hosts = cachedHosts;
      HostSpec candidateHost = getCandidateHostForInitialConnection();
      if (candidateHost != null) {
        connectToHost(candidateHost);
      }
    }
  }

  /**
   * Retrieves a host to use for the initial connection
   *
   * @return {@link HostSpec} for a candidate to use for the initial connection
   */
  @RequiresNonNull({"this.hosts", "this.topologyService"})
  private @Nullable HostSpec getCandidateHostForInitialConnection() {
    if (this.explicitlyReadOnly) {
      HostSpec candidateReader = getCandidateReaderForInitialConnection();
      if (candidateReader != null) {
        return candidateReader;
      }
    }
    return this.hosts.get(WRITER_CONNECTION_INDEX);
  }

  /**
   * Retrieves a reader to use for the initial connection
   *
   * @return {@link HostSpec} for a reader candidate to use for the initial connection
   */
  @RequiresNonNull({"this.topologyService", "this.hosts"})
  private @Nullable HostSpec getCandidateReaderForInitialConnection() {
    HostSpec lastUsedReader = topologyService.getLastUsedReaderHost();
    if (topologyContainsHost(lastUsedReader)) {
      return lastUsedReader;
    }

    return clusterContainsReader() ? getRandomReaderHost() : null;
  }

  /**
   * Checks if the topology information contains the given host
   *
   * @param host the host to check
   * @return true if the host exists in the topology
   */
  @RequiresNonNull({"this.hosts"})
  private boolean topologyContainsHost(@Nullable HostSpec host) {
    if (host == null) {
      return false;
    }
    for (HostSpec potentialMatch : this.hosts) {
      if (potentialMatch != null && potentialMatch.equals(host)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the cluster contains a reader
   *
   * @return true if there are readers in the cluster
   */
  @RequiresNonNull({"this.hosts"})
  private boolean clusterContainsReader() {
    return this.hosts.size() > 1;
  }

  /**
   * Retrieves a random host from the topology
   *
   * @return A random {@link HostSpec} from the topology
   */
  @RequiresNonNull({"this.hosts"})
  private HostSpec getRandomReaderHost() {
    int max = this.hosts.size() - 1;
    int min = WRITER_CONNECTION_INDEX + 1;
    int readerIndex = (int) (Math.random() * ((max - min) + 1)) + min;
    return this.hosts.get(readerIndex);
  }

  /**
   * Initializes topology information for the database we have connected to, when it is available
   *
   * @param hostSpec                     The {@link HostSpec} that we used to establish the
   *                                     current connection
   * @param connectedUsingCachedTopology a boolean representing whether the current connection
   *                                     was established using
   *                                     cached topology information
   */
  @RequiresNonNull({"this.topologyService", "this.rdsDnsAnalyzer", "this.hosts"})
  private synchronized void initTopology(
      HostSpec hostSpec,
      boolean connectedUsingCachedTopology) {
    final BaseConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (currentConnection != null) {
      List<HostSpec> topology = this.topologyService.getTopology(currentConnection, false);
      this.hosts = topology.isEmpty() ? this.hosts : topology;
    }

    this.isClusterTopologyAvailable = !this.hosts.isEmpty();

    // if we connected using the cached topology, there's a chance that the currentHost details
    // are stale, let's update it here.
    // Otherwise, we should try to set the currentHost if it isn't set already.
    if (connectedUsingCachedTopology || (currentConnection != null && this.currentHost == null)) {
      updateInitialHost(
          this.currentHost == null ? hostSpec : this.currentHost,
          connectedUsingCachedTopology);
    }
  }

  /**
   * Updates the current host in the class. Will set the currentHost to null if the given
   * hostSpec does not specify
   * the writer cluster and does not exist in the topology information.
   *
   * @param hostSpec                     The {@link HostSpec} that we used to establish the
   *                                     current connection
   * @param connectedUsingCachedTopology a boolean representing whether the current connection
   *                                     was established using
   *                                     cached topology information
   */
  @RequiresNonNull({"this.hosts", "this.rdsDnsAnalyzer"})
  // TODO: verify if this is necessary
  private void updateInitialHost(
      HostSpec hostSpec,
      boolean connectedUsingCachedTopology) {
    final String connUrlHost = hostSpec.getHost();
    if (!connectedUsingCachedTopology
        && this.rdsDnsAnalyzer.isWriterClusterDns(connUrlHost)
        && !this.hosts.isEmpty()) {
      this.currentHost = this.hosts.get(WRITER_CONNECTION_INDEX);
    } else {
      for (HostSpec host : this.hosts) {
        if (hostSpec.toString().equals(FailoverUtil.getHostPortPair(host))) {
          this.currentHost = host;
          return;
        }
      }
      // TODO: verify, keep stale host or set to null
      this.currentHost = null;
    }
  }

  /**
   * If failover is enabled, validate that the current connection is valid
   *
   * @param connectedUsingCachedTopology a boolean representing whether the current connection
   *                                     was established using
   *                                     cached topology information
   * @throws SQLException if an error occurs while validating the connection
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler",
      "this.readerFailoverHandler", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private void finalizeConnection(boolean connectedUsingCachedTopology) throws SQLException {
    if (isFailoverEnabled()) {
      // logTopology();
      // if (this.gatherPerfMetricsSetting) {
      //   this.failoverStartTimeMs = System.currentTimeMillis();
      // }
      validateInitialConnection(connectedUsingCachedTopology);
      if (this.currentHost != null && this.explicitlyReadOnly) {
        topologyService.setLastUsedReaderHost(this.currentHost);
      }

      final BaseConnection currentConnection =
          this.currentConnectionProvider.getCurrentConnection();
      if (currentConnection != null) {
        try {
          currentConnection.getQueryExecutor().setNetworkTimeout(this.failoverSocketTimeout * 1000);
        } catch (IOException e) {
          throw new SQLException(e.getMessage(), PSQLState.UNEXPECTED_ERROR.getState());
        }
      }
    }
  }

  /**
   * Validates the initial connection by making sure that we are connected and that we are not
   * connected to a reader
   * instance using a read-write connection, which could happen if we used stale cached topology
   * information to establish
   * the connection.
   *
   * @param connectedUsingCachedTopology a boolean representing whether the current connection
   *                                     was established using
   *                                     cached topology information
   * @throws SQLException if an error occurs during failover
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics",
      "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider",
      "this.hosts"})
  private synchronized void validateInitialConnection(boolean connectedUsingCachedTopology)
      throws SQLException {
    if (!isConnected()) {
      pickNewConnection();
      return;
    }

    if (!invalidCachedWriterConnection(connectedUsingCachedTopology)) {
      return;
    }

    try {
      connectToHost(this.hosts.get(WRITER_CONNECTION_INDEX));
    } catch (SQLException e) {
      failover();
    }
  }

  /**
   * If we used cached topology information to make the initial connection, checks whether we
   * connected to a host that was marked as a writer in the cache but is in fact a reader according
   * to the fresh topology. This could happen if the cached topology was stale.
   *
   * @param connectedUsingCachedTopology a boolean representing whether the current connection
   *                                     was established using
   *                                     cached topology information
   * @return true if we expected to be connected to a writer according to cached topology
   * information, but that information was stale and we are actually incorrectly connected to a
   * reader
   */
  private boolean invalidCachedWriterConnection(boolean connectedUsingCachedTopology) {
    if (this.explicitlyReadOnly || !connectedUsingCachedTopology) {
      return false;
    }

    final BaseConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();

    return (this.currentHost == null)
        || !this.currentHost.equals(
        this.topologyService.getTopology(currentConnection, false).get(WRITER_CONNECTION_INDEX));
  }

  /**
   * Configure the failover settings as well as the connect and socket timeout values
   *
   * @param props The {@link Properties} containing the settings for failover and socket/connect
   *              timeouts
   * @throws PSQLException if an integer property's string representation can't be converted to
   *                       an int. This should only
   *                       happen if the property's string is not an integer representation
   */
  private synchronized void initSettings(Properties props) throws PSQLException {

    this.clusterIdSetting = FailoverProperty.CLUSTER_ID.get(props);
    this.clusterInstanceHostPatternSetting =
        FailoverProperty.CLUSTER_INSTANCE_HOST_PATTERN.get(props);
    this.clusterTopologyRefreshRateMsSetting =
        FailoverProperty.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInt(props);

    this.failoverClusterTopologyRefreshRateMsSetting =
        FailoverProperty.FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInt(props);
    this.failoverReaderConnectTimeoutMsSetting =
        FailoverProperty.FAILOVER_READER_CONNECT_TIMEOUT_MS.getInt(props);
    this.failoverTimeoutMsSetting =
        FailoverProperty.FAILOVER_TIMEOUT_MS.getInt(props);
    this.failoverWriterReconnectIntervalMsSetting =
        FailoverProperty.FAILOVER_WRITER_RECONNECT_INTERVAL_MS.getInt(props);

    if (props.getProperty(PGProperty.CONNECT_TIMEOUT.getName(), null) != null) {
      this.failoverConnectTimeout = PGProperty.CONNECT_TIMEOUT.getInt(props);
    } else {
      this.failoverConnectTimeout = DEFAULT_CONNECT_TIMEOUT;
    }

    if (props.getProperty(PGProperty.SOCKET_TIMEOUT.getName(), null) != null) {
      this.failoverSocketTimeout = PGProperty.SOCKET_TIMEOUT.getInt(props);
    } else {
      this.failoverSocketTimeout = DEFAULT_SOCKET_TIMEOUT;
    }

    this.enableFailoverSetting = FailoverProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getBoolean(props);
    this.gatherPerfMetricsSetting = FailoverProperty.GATHER_PERF_METRICS.getBoolean(props);
  }

  /**
   * Checks if failover is enabled/possible.
   *
   * @return true if cluster-aware failover is enabled
   */
  public synchronized boolean isFailoverEnabled() {
    return this.enableFailoverSetting
        && !this.isRdsProxy
        && this.isClusterTopologyAvailable;
  }

  @Override
  public void releaseResources() {

  }

  boolean isRdsProxy() {
    return isRdsProxy;
  }

  boolean isRds() {
    return isRds;
  }

  boolean isClusterTopologyAvailable() {
    return isClusterTopologyAvailable;
  }
}
