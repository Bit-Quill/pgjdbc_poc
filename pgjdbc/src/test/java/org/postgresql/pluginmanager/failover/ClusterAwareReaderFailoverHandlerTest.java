/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

class ClusterAwareReaderFailoverHandlerTest {
  static final int numTestUrls = 6;
  static final String testUrl1 = "jdbc:postgresql:aws://writer-1:1234/";
  static final String testUrl2 = "jdbc:postgresql:aws://reader-1:2345/";
  static final String testUrl3 = "jdbc:postgresql:aws://reader-2:3456/";
  static final String testUrl4 = "jdbc:postgresql:aws://reader-3:4567/";
  static final String testUrl5 = "jdbc:postgresql:aws://reader-4:5678/";
  static final String testUrl6 = "jdbc:postgresql:aws://reader-5:6789/";
  private static final int WRITER_CONNECTION_INDEX = 0;

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

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testFailover() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: successful connection for host at index 4
    final BaseConnection mockConnection = Mockito.mock(BaseConnection.class);
    final List<HostSpec> hosts = getHostsFromTestUrls(6);
    final int currentHostIndex = 2;
    final int successHostIndex = 4;
    for (int i = 0; i < hosts.size(); i++) {
      HostSpec host = hosts.get(i);
      assertNotNull(FailoverUtil.getUrl(host));
      if (i != successHostIndex) {
        when(mockConnectionProvider.connect(eq(toHostSpec(host)), any(Properties.class),
            eq(FailoverUtil.getUrl(host)))).thenThrow(new SQLException());
      } else {
        when(mockConnectionProvider.connect(eq(toHostSpec(host)), any(Properties.class),
            eq(FailoverUtil.getUrl(host)))).thenReturn(mockConnection);
      }
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(FailoverUtil.getHostPortPair(hosts.get(hostIndex)));
    }
    when(mockTopologyService.getDownHosts()).thenReturn(downHosts);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties());
    final ReaderFailoverResult result = target.failover(hosts, hosts.get(currentHostIndex));

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(hosts.get(successHostIndex), result.getHost());

    final HostSpec successHost = hosts.get(successHostIndex);
    verify(mockTopologyService, atLeast(4)).addToDownHostList(any());
    verify(mockTopologyService, never()).addToDownHostList(eq(successHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(successHost));
  }

  private List<HostSpec> getHostsFromTestUrls(int numHosts) {
    final List<String> urlList =
        Arrays.asList(testUrl1, testUrl2, testUrl3, testUrl4, testUrl5, testUrl6);
    final List<HostSpec> hosts = new ArrayList<>();
    if (numHosts < 0 || numHosts > numTestUrls) {
      numHosts = numTestUrls;
    }

    for (int i = 0; i < numHosts; i++) {
      String url = urlList.get(i);
      int port = Integer.parseInt(url.substring(url.lastIndexOf(":") + 1, url.lastIndexOf("/")));
      String host = url.substring(url.indexOf("//") + 2, url.lastIndexOf("/"));
      hosts.add(new HostSpec(url, port));
    }
    return hosts;
  }

  @Test
  public void testFailover_timeout() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: failure to get reader since process is limited to 5s and each
    // attempt to connect takes 20s
    final List<HostSpec> hosts = getHostsFromTestUrls(6);
    final int currentHostIndex = 2;
    for (HostSpec host : hosts) {
      assertNotNull(FailoverUtil.getUrl(host));
      when(mockConnectionProvider.connect(eq(toHostSpec(host)), any(Properties.class),
          eq(FailoverUtil.getUrl(host))))
          .thenAnswer((Answer<BaseConnection>) invocation -> {
            Thread.sleep(20000);
            return mockConnection;
          });
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(FailoverUtil.getHostPortPair(hosts.get(hostIndex)));
    }
    when(mockTopologyService.getDownHosts()).thenReturn(downHosts);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties(), 5000, 30000);
    final ReaderFailoverResult result = target.failover(hosts, hosts.get(currentHostIndex));

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testFailover_emptyHostList() throws SQLException {
    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService, mockConnectionProvider, new Properties());
    final HostSpec currentHost = new HostSpec("writer", 1234);

    final List<HostSpec> hosts = new ArrayList<>();
    ReaderFailoverResult result = target.failover(hosts, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testGetReader_connectionSuccess() throws SQLException {
    // even number of connection attempts
    // first connection attempt to return succeeds, second attempt cancelled
    // expected test result: successful connection for host at index 2
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final List<HostSpec> hosts =
        getHostsFromTestUrls(3); // 2 connection attempts (writer not attempted)
    final HostSpec slowHost = hosts.get(1);
    assertNotNull(FailoverUtil.getUrl(slowHost));
    final HostSpec fastHost = hosts.get(2);
    assertNotNull(FailoverUtil.getUrl(fastHost));
    when(mockConnectionProvider.connect(eq(toHostSpec(slowHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(slowHost))))
        .thenAnswer(
            (Answer<BaseConnection>)
                invocation -> {
                  Thread.sleep(20000);
                  return mockConnection;
                });
    when(mockConnectionProvider.connect(eq(toHostSpec(fastHost)), any(Properties.class),
        eq(FailoverUtil.getUrl(fastHost)))).thenReturn(mockConnection);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties());
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(fastHost, result.getHost());

    verify(mockTopologyService, never()).addToDownHostList(any());
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(fastHost));
  }

  @Test
  public void testGetReader_connectionFailure() throws SQLException {
    // odd number of connection attempts
    // first connection attempt to return fails
    // expected test result: failure to get reader
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final List<HostSpec> hosts =
        getHostsFromTestUrls(4); // 3 connection attempts (writer not attempted)
    when(mockConnectionProvider.connect(any(), any(Properties.class), any(String.class))).thenThrow(
        new SQLException());

    final int currentHostIndex = 2;

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties());
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());

    final HostSpec currentHost = hosts.get(currentHostIndex);
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(eq(currentHost));
    verify(mockTopologyService, never())
        .addToDownHostList(
            eq(hosts.get(WRITER_CONNECTION_INDEX)));
  }

  @Test
  public void testGetReader_connectionAttemptsTimeout() throws SQLException {
    // connection attempts time out before they can succeed
    // first connection attempt to return times out
    // expected test result: failure to get reader
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final List<HostSpec> hosts =
        getHostsFromTestUrls(3); // 2 connection attempts (writer not attempted)
    when(mockConnectionProvider.connect(any(), any(Properties.class), any(String.class)))
        .thenAnswer(
            (Answer<BaseConnection>)
                invocation -> {
                  try {
                    Thread.sleep(5000);
                  } catch (InterruptedException exception) {
                    // ignore
                  }
                  return mockConnection;
                });

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties(), 60000, 1000);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());

    verify(mockTopologyService, never()).addToDownHostList(any());
  }

  @Test
  public void testGetHostsByPriority() {
    final List<HostSpec> originalHosts = getHostsFromTestUrls(6);

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(FailoverUtil.getHostPortPair(originalHosts.get(hostIndex)));
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties());
    final List<HostSpec> hostsByPriority =
        target.getHostsByPriority(originalHosts, downHosts);

    assertPriorityOrdering(hostsByPriority, downHosts, true);
    assertEquals(6, hostsByPriority.size());
  }

  @Test
  public void testGetReaderHostsByPriority() {
    final List<HostSpec> originalHosts = getHostsFromTestUrls(6);

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(FailoverUtil.getHostPortPair(originalHosts.get(hostIndex)));
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnectionProvider,
            new Properties());
    final List<HostSpec> readerHosts =
        target.getReaderHostsByPriority(originalHosts, downHosts);

    // assert the following priority ordering: active readers, down readers
    assertPriorityOrdering(readerHosts, downHosts, false);
    assertEquals(5, readerHosts.size());
  }

  private void assertPriorityOrdering(List<HostSpec> hostsByPriority, Set<String> downHosts,
      boolean shouldContainWriter) {
    // assert the following priority ordering: active readers, writer, down readers
    int i;
    int numActiveReaders = 2;
    for (i = 0; i < numActiveReaders; i++) {
      HostSpec activeReader = hostsByPriority.get(i);
      // assertFalse(activeReader.isWriter());
      assertFalse(downHosts.contains(FailoverUtil.getHostPortPair(activeReader)));
    }
    if (shouldContainWriter) {
      HostSpec writerHost = hostsByPriority.get(i);
      // assertTrue(writerHost.isWriter());
      i++;
    }
    for (; i < hostsByPriority.size(); i++) {
      HostSpec downReader = hostsByPriority.get(i);
      // assertFalse(downReader.isWriter());
      assertTrue(downHosts.contains(FailoverUtil.getHostPortPair(downReader)));
    }
  }

  private HostSpec[] toHostSpec(final HostSpec hostSpec) {
    return new HostSpec[]{hostSpec};
  }
}
