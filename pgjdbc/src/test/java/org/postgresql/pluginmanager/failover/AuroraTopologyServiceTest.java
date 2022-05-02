/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.postgresql.util.HostSpec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class AuroraTopologyServiceTest {

  @Mock Connection mockConnection;
  @Spy AuroraTopologyService spyTopologyService;
  @Mock ResultSet mockResults;
  @Mock Statement mockStatement;

  private static final int PORT = 5432;
  private static final int NO_PORT = -1;
  private static final String CLUSTER_INSTANCE_HOST_PATTERN = "?.XYZ.us-east-2.rds.amazonaws.com";

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    AuroraTopologyService.topologyCache.clear();
    closeable.close();
  }

  @Test
  public void testTopologyQuery() throws SQLException {
    stubTopologyQuery(mockConnection);

    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    final List<HostSpec> topology = spyTopologyService.getTopology(mockConnection, false);
    assertNotNull(topology);

    final HostSpec writer = topology.get(AuroraTopologyService.WRITER_CONNECTION_INDEX);
    final List<HostSpec> readers =
        topology.subList(AuroraTopologyService.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals("writer-instance.XYZ.us-east-2.rds.amazonaws.com", writer.getHost());
    assertEquals(PORT, writer.getPort());
    // assertTrue(writer.isWriter());

    assertEquals(3, topology.size());
    assertEquals(2, readers.size());
  }

  @Test
  public void testTopologyQuery_MultiWriter() throws SQLException {
    stubTopologyQueryMultiWriter(mockConnection);

    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);

    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    final List<HostSpec> topology = spyTopologyService.getTopology(mockConnection, false);
    assertNotNull(topology);
    final List<HostSpec> readers =
        topology.subList(AuroraTopologyService.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals(3, topology.size());
    assertEquals(2, readers.size());

    final HostSpec writer1 = topology.get(AuroraTopologyService.WRITER_CONNECTION_INDEX);
    final HostSpec writer2 = topology.get(2);
    final HostSpec reader = topology.get(1);

    assertEquals("writer-instance-1.XYZ.us-east-2.rds.amazonaws.com", writer1.getHost());
    assertEquals(PORT, writer1.getPort());
    // assertEquals("writer-instance-1", writer1.getInstanceIdentifier());
    // assertTrue(writer1.isWriter());

    assertEquals("writer-instance-2.XYZ.us-east-2.rds.amazonaws.com", writer2.getHost());
    assertEquals(PORT, writer2.getPort());
    // assertEquals("writer-instance-2", writer2.getInstanceIdentifier());
    // A second writer indicates the topology is in a failover state, and the second writer is
    // the obsolete one. It will be come a reader shortly, so we mark it as such
    // assertFalse(writer2.isWriter());

    assertEquals("reader-instance.XYZ.us-east-2.rds.amazonaws.com", reader.getHost());
    assertEquals(PORT, reader.getPort());
    // assertEquals("reader-instance", reader.getInstanceIdentifier());
    // assertFalse(reader.isWriter());
  }

  private void stubTopologyQuery(Connection conn)
      throws SQLException {
    stubTopologyQueryExecution(conn, mockResults);
    stubTopologyResponseData(mockResults);
  }

  private void stubTopologyQueryMultiWriter(Connection conn)
      throws SQLException {
    stubTopologyQueryExecution(conn, mockResults);
    stubTopologyResponseDataMultiWriter(mockResults);
  }

  private void stubTopologyQueryExecution(Connection conn, ResultSet results)
      throws SQLException {
    when(conn.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(AuroraTopologyService.RETRIEVE_TOPOLOGY_SQL)).thenReturn(
        results);
  }

  private void stubTopologyResponseData(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    // results.getString(AuroraTopologyService.SESSION_ID_COL) is called twice for each instance
    // and should return the same result both times
    when(results.getString(AuroraTopologyService.SESSION_ID_COL))
        .thenReturn(
            "Replica",
            "Replica",
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            "Replica",
            "Replica");
    when(results.getString(AuroraTopologyService.SERVER_ID_COL))
        .thenReturn(
            "replica-instance-1",
            "writer-instance",
            "replica-instance-2");
  }

  private void stubTopologyResponseDataMultiWriter(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    // results.getString(AuroraTopologyService.SESSION_ID_COL) is called twice for each instance
    // and should return the same result both times
    when(results.getString(AuroraTopologyService.SESSION_ID_COL))
        .thenReturn(
            "Replica",
            "Replica",
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID);
    when(results.getString(AuroraTopologyService.SERVER_ID_COL))
        .thenReturn(
            "reader-instance",
            "writer-instance-1",
            "writer-instance-2");
  }

  @Test
  public void testCachedEntryRetrieved() throws SQLException {
    stubTopologyQueryMultiWriter(mockConnection);
    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    spyTopologyService.getTopology(mockConnection, false);
    spyTopologyService.getTopology(mockConnection, false);

    verify(spyTopologyService, times(1)).queryForTopology(mockConnection);
  }

  @Test
  public void testForceUpdateQueryFailure() throws SQLException {
    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);
    when(mockConnection.createStatement()).thenThrow(SQLException.class);

    final List<HostSpec> hosts = spyTopologyService.getTopology(mockConnection, true);
    assertTrue(hosts.isEmpty());
  }

  @Test
  public void testQueryFailureReturnsStaleTopology() throws SQLException, InterruptedException {
    stubTopologyQuery(mockConnection);
    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);
    spyTopologyService.setRefreshRate(1);

    final List<HostSpec> hosts = spyTopologyService.getTopology(mockConnection, false);
    when(mockConnection.createStatement()).thenThrow(SQLException.class);
    Thread.sleep(5);
    final List<HostSpec> staleHosts = spyTopologyService.getTopology(mockConnection, false);
    assertNotNull(staleHosts);

    verify(spyTopologyService, times(2)).queryForTopology(mockConnection);
    assertEquals(3, staleHosts.size());
    assertEquals(hosts, staleHosts);
  }

  @Test
  public void testProviderRefreshesTopology() throws SQLException, InterruptedException {
    stubTopologyQueryMultiWriter(mockConnection);

    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    spyTopologyService.setRefreshRate(1);
    spyTopologyService.getTopology(mockConnection, false);
    Thread.sleep(2);
    spyTopologyService.getTopology(mockConnection, false);

    verify(spyTopologyService, times(2)).queryForTopology(mockConnection);
  }

  @Test
  public void testProviderTopologyExpires() throws SQLException, InterruptedException {
    stubTopologyQueryMultiWriter(mockConnection);

    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    AuroraTopologyService.setExpireTime(1000); // 1 sec
    spyTopologyService.setRefreshRate(
        10000); // 10 sec; and cache expiration time is also (indirectly) changed to 10 sec

    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(1)).queryForTopology(mockConnection);

    Thread.sleep(3000);

    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(1)).queryForTopology(mockConnection);

    Thread.sleep(3000);
    // internal cache has NOT expired yet
    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(1)).queryForTopology(mockConnection);

    Thread.sleep(5000);
    // internal cache has expired by now
    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(2)).queryForTopology(mockConnection);
  }

  @Test
  public void testProviderTopologyNotExpired() throws SQLException, InterruptedException {
    stubTopologyQueryMultiWriter(mockConnection);

    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    AuroraTopologyService.setExpireTime(10000); // 10 sec
    spyTopologyService.setRefreshRate(1000); // 1 sec

    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(1)).queryForTopology(mockConnection);

    Thread.sleep(2000);

    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(2)).queryForTopology(mockConnection);

    Thread.sleep(2000);

    spyTopologyService.getTopology(mockConnection, false);
    verify(spyTopologyService, times(3)).queryForTopology(mockConnection);
  }

  @Test
  public void testClearProviderCache() throws SQLException {
    stubTopologyQueryMultiWriter(mockConnection);

    final HostSpec clusterHostInfo = new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN, PORT);
    spyTopologyService.setClusterInstanceTemplate(clusterHostInfo);

    spyTopologyService.getTopology(mockConnection, false);
    spyTopologyService.addToDownHostList(clusterHostInfo);
    assertEquals(1, AuroraTopologyService.topologyCache.size());

    spyTopologyService.clearAll();
    assertEquals(0, AuroraTopologyService.topologyCache.size());
  }
}
