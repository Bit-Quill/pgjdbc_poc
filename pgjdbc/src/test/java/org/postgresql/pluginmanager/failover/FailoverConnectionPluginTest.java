/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.postgresql.Driver;
import org.postgresql.PGProperty;
import org.postgresql.PGStatement;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.TransactionState;
import org.postgresql.pluginmanager.IConnectionProvider;
import org.postgresql.pluginmanager.ICurrentConnectionProvider;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationMode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

class FailoverConnectionPluginTest {

  @Mock IConnectionProvider mockConnectionProvider;
  @Mock ICurrentConnectionProvider mockCurrentConnectionProvider;
  @Mock BaseConnection mockConnection;
  @Mock HostSpec mockHostSpec;
  @Mock Callable<?> mockCallable;
  @Mock QueryExecutor mockQueryExecutor;
  @Mock ITopologyService mockTopologyService;
  @Mock ClusterAwareReaderFailoverHandler mockReaderFailoverHandler;
  @Mock ClusterAwareWriterFailoverHandler mockWriterFailoverHandler;
  @Mock ReaderFailoverResult mockReaderFailoverResult;
  @Mock WriterFailoverResult mockWriterFailoverResult;
  @Mock RdsDnsAnalyzer mockDnsAnalyzer;
  @Mock HostSpec mockHostInfo;
  @Captor ArgumentCaptor<HostSpec[]> hostSpecCaptor;

  private static final String HOST = "my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final int PORT = 5432;
  private static final String TEST_DB = "test";
  private static final String URL =
      "jdbc:postgresql://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final String URL_WITH_DB = URL + "/" + TEST_DB;
  private static final String HOST_PATTERN = "?.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockConnection);
    when(mockCurrentConnectionProvider.getCurrentHostSpec()).thenReturn(mockHostSpec);
    when(mockConnection.getQueryExecutor()).thenReturn(mockQueryExecutor);
    when(mockConnection.getCatalog()).thenReturn(TEST_DB);
    when(mockHostSpec.getHost()).thenReturn(HOST);
    when(mockHostSpec.getPort()).thenReturn(PORT);
    when(mockTopologyService.getCachedTopology()).thenReturn(
        Collections.singletonList(mockHostInfo));
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(
        Collections.singletonList(mockHostInfo));
    when(mockConnectionProvider.connect(any(), any(), any())).thenReturn(mockConnection);
    when(mockWriterFailoverHandler.failover(any())).thenReturn(mockWriterFailoverResult);
    when(mockReaderFailoverHandler.failover(any(), any())).thenReturn(mockReaderFailoverResult);
    when(mockQueryExecutor.getTransactionState()).thenReturn(TransactionState.IDLE);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testTriggerFailoverDuringInitialConnection() throws Exception {
    when(mockDnsAnalyzer.getRdsInstanceHostPattern(eq(HOST))).thenReturn(HOST_PATTERN);
    when(mockDnsAnalyzer.isRdsClusterDns(eq(HOST_PATTERN))).thenReturn(Boolean.TRUE);
    when(mockDnsAnalyzer.isReaderClusterDns(eq(HOST_PATTERN))).thenReturn(Boolean.FALSE);
    when(mockConnection.isClosed()).thenReturn(Boolean.TRUE);
    when(mockWriterFailoverResult.isConnected()).thenReturn(Boolean.TRUE);
    when(mockWriterFailoverResult.getTopology()).thenReturn(
        Collections.singletonList(mockHostInfo));
    when(mockWriterFailoverResult.getNewConnection()).thenReturn(mockConnection);
    when(mockCallable.call())
        .thenThrow(createSQLException(PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState()));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    assertThrows(SQLException.class, () -> plugin.openInitialConnection(
        new HostSpec[]{mockHostSpec},
        properties,
        "connection url",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        mockDnsAnalyzer
    ));
  }

  @Test
  public void testExecuteThrowsSQLExceptionTriggerFailover() throws Exception {
    when(mockDnsAnalyzer.getRdsInstanceHostPattern(eq(HOST))).thenReturn(HOST_PATTERN);
    when(mockDnsAnalyzer.isRdsClusterDns(eq(HOST_PATTERN))).thenReturn(Boolean.TRUE);
    when(mockDnsAnalyzer.isReaderClusterDns(eq(HOST_PATTERN))).thenReturn(Boolean.FALSE);
    when(mockDnsAnalyzer.isRdsDns(HOST)).thenReturn(Boolean.TRUE);
    when(mockQueryExecutor.getTransactionState()).thenReturn(TransactionState.IDLE);
    when(mockConnection.isClosed()).thenReturn(Boolean.FALSE);
    when(mockWriterFailoverResult.isConnected()).thenReturn(Boolean.TRUE);
    when(mockWriterFailoverResult.getTopology()).thenReturn(
        Collections.singletonList(mockHostInfo));
    when(mockWriterFailoverResult.getNewConnection()).thenReturn(mockConnection);
    when(mockCallable.call())
        .thenThrow(createSQLException(PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState()));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    plugin.openInitialConnection(
        new HostSpec[]{mockHostSpec},
        properties,
        "connection url",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        mockDnsAnalyzer
    );

    when(mockConnection.isClosed()).thenReturn(Boolean.TRUE);
    assertThrows(SQLException.class, () -> plugin.execute(
        PGStatement.class,
        "Statement.executeQuery",
        mockCallable,
        new String[]{"select 1"}
    ));
  }

  /**
   * Tests {@link FailoverConnectionPlugin} return original connection if failover is not
   * enabled.
   */
  @Test
  public void testFailoverDisabled() throws Exception {
    final String url = "jdbc:postgresql://somehost:1234";
    final Properties properties = new Properties();
    properties.setProperty(FailoverProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getName(), "false");
    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        url,
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        mockDnsAnalyzer
    );

    assertFalse(plugin.enableFailoverSetting);
    verify(mockConnectionProvider, only()).connect(eq(hostSpecs), eq(properties), eq(url));
  }

  @Test
  public void testIfClusterTopologyAvailable() throws Exception {
    final String url = "jdbc:postgresql://somehost:1234";
    final Properties properties = new Properties();
    properties.setProperty(FailoverProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName(), "?.somehost");

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        url,
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        mockDnsAnalyzer
    );

    assertFalse(plugin.isRds());
    assertFalse(plugin.isRdsProxy());
    assertTrue(plugin.isClusterTopologyAvailable());
    assertTrue(plugin.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testClusterTopologyNotAvailable() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty(FailoverProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName(), "?.somehost");
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
        .thenReturn(new ArrayList<>());

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        URL_WITH_DB,
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        mockDnsAnalyzer
    );

    assertTrue(plugin.isConnected());
    assertFalse(plugin.isRds());
    assertFalse(plugin.isRdsProxy());
    assertFalse(plugin.isClusterTopologyAvailable());
    assertFalse(plugin.isFailoverEnabled());
  }

  @Test
  public void testIfClusterTopologyAvailableAndDnsPatternRequired() {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    assertThrows(SQLException.class, () -> plugin.openInitialConnection(
        hostSpecs,
        properties,
        URL_WITH_DB,
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        mockDnsAnalyzer
    ));
  }

  @ParameterizedTest
  @MethodSource("provideRdsEndpoints")
  public void testRdsEndpoints(String clusterURL, VerificationMode mode, String clusterId)
      throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertTrue(plugin.isRds());
    assertFalse(plugin.isRdsProxy());
    assertTrue(plugin.isClusterTopologyAvailable());
    assertTrue(plugin.isFailoverEnabled());
    assertTrue(plugin.isConnected());

    if ("".equals(clusterId)) {
      verify(mockTopologyService, mode).setClusterId(any());
    } else {
      verify(mockTopologyService, mode).setClusterId(clusterId);
    }
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  private static Stream<Arguments> provideRdsEndpoints() {
    return Stream.of(
        // RDS cluster endpoint
        Arguments.of("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com", atLeastOnce(), "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234"),
        // RDS read-only cluster endpoint
        Arguments.of("my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com", atLeastOnce(), "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234"),
        // RDS custom cluster endpoint
        Arguments.of("my-custom-cluster-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com", never(), ""),
        // RDS instance endpoint
        Arguments.of("my-instance-name.XYZ.us-east-2.rds.amazonaws.com", never(), "")
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // Custom domain cluster endpoint
      "my-custom-domain.com",
      // IP Address cluster
      "10.10.10.10"
  })
  public void testCustomEndpointCluster(String clusterURL) throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);
    properties.put(FailoverProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName(), "?.my-custom-domain.com:9999");

    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertFalse(plugin.isRds());
    assertFalse(plugin.isRdsProxy());
    assertTrue(plugin.isClusterTopologyAvailable());
    assertTrue(plugin.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsProxy() throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final String clusterURL = "test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertTrue(plugin.isRds());
    assertTrue(plugin.isRdsProxy());
    assertTrue(plugin.isClusterTopologyAvailable());
    assertFalse(plugin.isFailoverEnabled());
    assertTrue(plugin.isConnected());

    verify(mockTopologyService, atLeastOnce())
        .setClusterId("test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressClusterWithClusterId() throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);
    properties.put(FailoverProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName(), "?.my-custom-domain.com:9999");
    properties.put(FailoverProperty.CLUSTER_ID.getName(), "test-cluster-id");

    final String clusterURL = "10.10.10.10";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertFalse(plugin.isRds());
    assertFalse(plugin.isRdsProxy());
    assertTrue(plugin.isClusterTopologyAvailable());
    assertTrue(plugin.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterId("test-cluster-id");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressAndTopologyAvailableAndDnsPatternRequired() throws Exception {
    final Properties properties = new Properties();

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    when(mockHostSpec.getHost()).thenReturn("10.10.10.10");
    when(mockHostSpec.getPort()).thenReturn(1234);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    assertThrows(SQLException.class, () ->
        plugin.openInitialConnection(
            hostSpecs,
            properties,
            "",
            () -> null,
            mockConnectionProvider,
            () -> mockTopologyService,
            () -> mockReaderFailoverHandler,
            () -> mockWriterFailoverHandler,
            new RdsDnsAnalyzer()
        ));
  }

  @Test
  public void testIpAddressAndTopologyNotAvailable() throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final String clusterURL = "10.10.10.10";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final List<HostSpec> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertFalse(plugin.isRds());
    assertFalse(plugin.isRdsProxy());
    assertFalse(plugin.isClusterTopologyAvailable());
    assertFalse(plugin.isFailoverEnabled());
  }

  @Test
  public void testReadOnlyFalseWhenWriterCluster() throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final String clusterURL = "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec writerHost = new HostSpec("writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234);

    final List<HostSpec> topology = Arrays.asList(
        new HostSpec("writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234),
        new HostSpec("reader-a-host.XYZ.us-east-2.rds.amazonaws.com", 1234),
        new HostSpec("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", 1234)
    );

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(topology);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertTrue(plugin.isConnected());
    assertEquals(writerHost, plugin.currentHost);
    assertFalse(plugin.explicitlyReadOnly);
  }

  @Test
  public void testReadOnlyTrueWhenReaderCluster() throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final String clusterURL = "my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final List<HostSpec> topology = Arrays.asList(
        new HostSpec("writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234),
        new HostSpec("reader-a-host.XYZ.us-east-2.rds.amazonaws.com", 1234)
    );

    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(topology);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertTrue(plugin.isConnected());
    assertNull(plugin.currentHost);
    assertTrue(plugin.explicitlyReadOnly);
  }

  @Test
  public void testLastUsedReaderAvailable() throws Exception {
    final Properties properties = new Properties();
    properties.put(PGProperty.PG_DBNAME.getName(), TEST_DB);

    final String clusterURL = "my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    final HostSpec readerA_Host =  new HostSpec("reader-a-host.XYZ.us-east-2.rds.amazonaws.com", 1234);
    final List<HostSpec> topology = Arrays.asList(
        new HostSpec("writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234),
        readerA_Host,
        new HostSpec("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", 1234)
    );

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getLastUsedReaderHost()).thenReturn(readerA_Host);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    assertTrue(plugin.isConnected());
    assertEquals(readerA_Host, plugin.currentHost);
    assertTrue(plugin.explicitlyReadOnly);
  }

  @Test
  public void testForWriterReconnectWhenInvalidCachedWriterConnection() throws Exception {
    final Properties properties = new Properties();
    final String clusterURL = "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final int clusterPort = 1234;

    when(mockHostSpec.getHost()).thenReturn(clusterURL);
    when(mockHostSpec.getPort()).thenReturn(clusterPort);

    final HostSpec cachedWriterHost = new HostSpec("cached-writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234);
    final HostSpec readerA_Host = new HostSpec("reader-a-host.XYZ.us-east-2.rds.amazonaws.com", 1234);

    final List<HostSpec> cachedTopology = Arrays.asList(
        cachedWriterHost,
        readerA_Host,
        new HostSpec("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", 1234)
    );

    final HostSpec actualWriterHost = new HostSpec("actual-writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234);
    // cached writer is now actually a reader
    final HostSpec updatedCachedWriterHost = new HostSpec("cached-writer-host.XYZ.us-east-2.rds.amazonaws.com", 1234);

    final List<HostSpec> actualTopology = Arrays.asList(
        actualWriterHost,
        readerA_Host,
        updatedCachedWriterHost
    );

    when(mockTopologyService.getCachedTopology()).thenReturn(cachedTopology);
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(actualTopology);

    final HostSpec[] hostSpecs = new HostSpec[]{mockHostSpec};

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        properties,
        mockConnectionProvider);

    plugin.openInitialConnection(
        hostSpecs,
        properties,
        "",
        () -> null,
        mockConnectionProvider,
        () -> mockTopologyService,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler,
        new RdsDnsAnalyzer()
    );

    verify(mockConnectionProvider, atLeast(2)).connect(hostSpecCaptor.capture(),any(Properties.class), any(String.class));
    final List<HostSpec[]> values = hostSpecCaptor.getAllValues();
    assertEquals(cachedWriterHost, values.get(0)[0]);
    assertEquals(actualWriterHost, values.get(1)[0]);

    assertTrue(plugin.isConnected());
    assertEquals(actualWriterHost, plugin.currentHost);
    assertFalse(plugin.explicitlyReadOnly);
  }

  private SQLException createSQLException(String sqlState) {
    return new SQLException(
        "exception",
        sqlState,
        new Throwable());
  }
}
