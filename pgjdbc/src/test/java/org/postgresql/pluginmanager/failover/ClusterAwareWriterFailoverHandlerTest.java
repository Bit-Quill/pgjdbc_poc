/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.QueryExecutor;
import org.postgresql.pluginmanager.IConnectionProvider;
import org.postgresql.pluginmanager.ICurrentConnectionProvider;
import org.postgresql.util.HostSpec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

class ClusterAwareWriterFailoverHandlerTest {

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
  @Mock BaseConnection mockWriterConnection;
  @Mock BaseConnection mockReaderA_Connection;

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * fail to connect to any reader due to exception expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBReaderException() throws SQLException {
    final HostSpec writerHost = createBasicHostInfo("writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");

    final List<HostSpec> currentTopology = Arrays.asList(writerHost, readerA_Host, readerB_Host);

    when(mockConnectionProvider.connect(eq(toHostSpec(writerHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(writerHost)))).thenReturn(mockConnection);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerA_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerA_Host)))).thenThrow(SQLException.class);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerB_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenThrow(SQLException.class);
    when(mockTopologyService.getTopology(any(BaseConnection.class), eq(true))).thenReturn(
        currentTopology);
    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList())).thenThrow(
        SQLException.class);

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(eq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: successfully re-connect to initial writer; return new connection taskB: successfully
   * connect to readerA and then new writer, but it takes more time than taskA expected test result:
   * new connection by taskA
   */
  @Test
  public void testReconnectToWriter_SlowReaderA() throws SQLException {
    final HostSpec writerHost = createBasicHostInfo("writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");

    final List<HostSpec> currentTopology = Arrays.asList(writerHost, readerA_Host, readerB_Host);

    final HostSpec newWriterHost = createBasicHostInfo("new-writer-host");
    final List<HostSpec> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(eq(toHostSpec(writerHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(writerHost)))).thenReturn(mockWriterConnection);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerB_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(eq(mockWriterConnection), eq(true)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockReaderA_Connection), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList()))
        .thenAnswer(
            (Answer<ReaderFailoverResult>)
                invocation -> {
                  Thread.sleep(5000);
                  return new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true);
                });

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(eq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * successfully connect to readerA and retrieve topology, but latest writer is not new (defer to
   * taskA) expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBDefers() throws SQLException {
    final HostSpec writerHost = createBasicHostInfo("writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");
    final List<HostSpec> currentTopology = Arrays.asList(writerHost, readerA_Host, readerB_Host);

    when(mockConnectionProvider.connect(eq(toHostSpec(writerHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(writerHost))))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockConnectionProvider.connect(eq(toHostSpec(readerB_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(BaseConnection.class), eq(true)))
        .thenReturn(currentTopology);

    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            60000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(eq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB, taskA sees no changes
   * taskA: successfully re-connect to writer; return connection to initial writer, but it takes
   * more
   * time than taskB taskB: successfully connect to readerA and then to new-writer expected test
   * result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_SlowWriter() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockNewWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailoverHandler =
        Mockito.mock(ReaderFailoverHandler.class);

    final HostSpec writerHost = createBasicHostInfo("writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");
    final List<HostSpec> currentTopology = Arrays.asList(writerHost, readerA_Host, readerB_Host);

    final HostSpec newWriterHost = createBasicHostInfo("new-writer-host");
    final List<HostSpec> newTopology = Arrays.asList(newWriterHost, readerA_Host, readerB_Host);

    when(mockConnectionProvider.connect(toHostSpec(writerHost), new Properties(),
        FailoverUtil.getUrl(writerHost)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockConnectionProvider.connect(eq(toHostSpec(readerA_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerB_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(eq(toHostSpec(newWriterHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(newWriterHost)))).thenReturn(mockNewWriterConnection);

    when(mockTopologyService.getTopology(eq(mockWriterConnection), eq(true)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockReaderA_Connection), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(3, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(writerHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(newWriterHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, initial-writer, reader-A, reader-B] taskA: successfully
   * reconnect, but initial-writer is now a reader (defer to taskB) taskB: successfully connect to
   * readerA and then to new-writer expected test result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_taskADefers() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final BaseConnection mockNewWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailoverHandler =
        Mockito.mock(ReaderFailoverHandler.class);

    final HostSpec initialWriterHost = createBasicHostInfo("initial-writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");

    final List<HostSpec> currentTopology =
        Arrays.asList(initialWriterHost, readerA_Host, readerB_Host);

    final HostSpec newWriterHost = createBasicHostInfo("new-writer-host");
    final List<HostSpec> newTopology =
        Arrays.asList(newWriterHost, initialWriterHost, readerA_Host, readerB_Host);

    when(mockConnectionProvider.connect(eq(toHostSpec(initialWriterHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(initialWriterHost))))
        .thenReturn(Mockito.mock(BaseConnection.class));
    when(mockConnectionProvider.connect(eq(toHostSpec(readerA_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerA_Host)))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerB_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(eq(toHostSpec(newWriterHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(newWriterHost))))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockNewWriterConnection;
                });

    when(mockTopologyService.getTopology(any(BaseConnection.class), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(4, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(initialWriterHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(newWriterHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: fail to re-connect to writer due to failover timeout taskB: successfully connect to
   * readerA and then fail to connect to writer due to failover timeout expected test result: no
   * connection
   */
  @Test
  public void testFailedToConnect_failoverTimeout() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockNewWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailoverHandler =
        Mockito.mock(ReaderFailoverHandler.class);

    final HostSpec writerHost = createBasicHostInfo("writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");
    final List<HostSpec> currentTopology = Arrays.asList(writerHost, readerA_Host, readerB_Host);

    final HostSpec newWriterHost = createBasicHostInfo("new-writer-host");
    final List<HostSpec> newTopology = Arrays.asList(newWriterHost, readerA_Host, readerB_Host);

    when(mockConnectionProvider.connect(toHostSpec(writerHost), new Properties(),
        FailoverUtil.getUrl(writerHost)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockWriterConnection;
                });
    when(mockConnectionProvider.connect(toHostSpec(readerA_Host), new Properties(),
        FailoverUtil.getUrl(readerA_Host))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(toHostSpec(readerB_Host), new Properties(),
        FailoverUtil.getUrl(readerB_Host))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(toHostSpec(newWriterHost), new Properties(),
        FailoverUtil.getUrl(newWriterHost)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockNewWriterConnection;
                });

    when(mockTopologyService.getTopology(eq(mockWriterConnection), any(Boolean.class)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockNewWriterConnection), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB taskA: fail to re-connect
   * to
   * writer due to exception taskB: successfully connect to readerA and then fail to connect to
   * writer due to exception expected test result: no connection
   */
  @Test
  public void testFailedToConnect_taskAException_taskBWriterException() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailoverHandler =
        Mockito.mock(ReaderFailoverHandler.class);

    final HostSpec writerHost = createBasicHostInfo("writer-host");
    final HostSpec readerA_Host = createBasicHostInfo("reader-a-host");
    final HostSpec readerB_Host = createBasicHostInfo("reader-b-host");
    final List<HostSpec> currentTopology = Arrays.asList(writerHost, readerA_Host, readerB_Host);

    final HostSpec newWriterHost = createBasicHostInfo("new-writer-host");
    final List<HostSpec> newTopology = Arrays.asList(newWriterHost, readerA_Host, readerB_Host);

    when(mockConnectionProvider.connect(eq(toHostSpec(writerHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(writerHost)))).thenThrow(SQLException.class);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerA_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerA_Host)))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(eq(toHostSpec(readerB_Host)), any(Properties.class),
        eq(FailoverUtil.getUrl(readerB_Host)))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(eq(toHostSpec(newWriterHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(newWriterHost)))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(BaseConnection.class), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailoverHandler.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailoverHandler,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(writerHost));
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(eq(newWriterHost));
  }

  private HostSpec createBasicHostInfo(String host) {
    return new HostSpec(host, 1234);
  }

  private HostSpec[] toHostSpec(final HostSpec hostSpec) {
    return new HostSpec[]{hostSpec};
  }
}
