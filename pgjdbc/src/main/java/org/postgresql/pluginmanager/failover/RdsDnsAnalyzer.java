/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.pluginmanager.failover;

import static org.postgresql.pluginmanager.failover.AuroraTopologyService.NO_PORT;

import org.postgresql.util.HostSpec;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RdsDnsAnalyzer {
  private static final Pattern URL_PATTERN = Pattern.compile("^(?<host>.*?)(?::(?<port>[^:]*))?$");
  private final Pattern auroraDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?([a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\"
              + ".rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraCustomClusterPattern =
      Pattern.compile(
          "(.+)\\.(cluster-custom-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraProxyDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  /**
   * Checks if the connection is a standard RDS DNS connection
   *
   * @param host The host of the connection
   * @return True if the connection contains the standard RDS DNS pattern
   */
  public boolean isRdsDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return matcher.find();
  }

  /**
   * Checks if the connection is Proxy DNS connection
   *
   * @param host The host of the connection
   * @return True if the connection contains a Proxy DNS pattern
   */
  public boolean isRdsProxyDns(String host) {
    Matcher matcher = auroraProxyDnsPattern.matcher(host);
    return matcher.find();
  }

  /**
   * Checks if the host is an RDS cluster using DNS
   *
   * @param host The host of the connection
   * @return True if the host is an RDS cluster using DNS
   */
  public boolean isRdsClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    return "cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword);
  }

  /**
   * Used to get a specific keyword from an instance
   *
   * @param matcher The matcher object that contains the string to parse
   * @return The cluster keyword
   */
  private @Nullable String getClusterKeyword(Matcher matcher) {
    if (matcher.find() && matcher.groupCount() >= 2
        && matcher.group(2) != null
        && matcher.group(1) != null) {
      String group1 = matcher.group(1);
      boolean isGroup1NotEmpty;
      if (group1 == null) {
        return null;
      } else {
        isGroup1NotEmpty = !group1.isEmpty();
      }

      if (isGroup1NotEmpty) {
        return matcher.group(2);
      }
    }
    return null;
  }

  /**
   * Checks if the host is connected to a writer cluster
   *
   * @param host The host of the connection
   * @return True if the host is connected to the writer cluster
   */
  public boolean isWriterClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  /**
   * Checks if the host is connected to a read-only cluster
   *
   * @param host The host of the connection
   * @return True if the host is read-only
   */
  public boolean isReaderClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-ro-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  /**
   * Checks if the connection is a custom cluster name
   *
   * @param host The host of the connection
   * @return True if the host is a custom cluster name
   */
  public boolean isRdsCustomClusterDns(String host) {
    Matcher matcher = auroraCustomClusterPattern.matcher(host);
    return matcher.find();
  }

  /**
   * Retrieve the instance host pattern from the host
   *
   * @param host The host of the connection
   * @return The instance host pattern that will be used to set the cluster instance template in the
   * topology service
   */
  public @Nullable String getRdsInstanceHostPattern(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    if (matcher.find() && matcher.groupCount() >= 3) {
      return "?." + matcher.group(3);
    }
    return null;
  }

  /**
   * Retrieves the cluster host URl from a connection string
   *
   * @param host The host of the connection
   * @return the cluster host URL from the connection string
   */
  public @Nullable String getRdsClusterHostUrl(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    if (("cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword)) && matcher.groupCount() >= 3) {
      return matcher.group(1) + ".cluster-" + matcher.group(3); // always RDS cluster endpoint
    }
    return null;
  }

  /**
   * Splits a URL into its host/port components and returns this information as a {@link HostSpec}
   *
   * @param url the URL to process
   * @return a {@link HostSpec} representing the host/port components of the given URL, or null if
   * there was a problem parsing the URL
   */
  public static @Nullable HostSpec parseUrl(@Nullable String url) {
    if (isNullOrEmpty(url)) {
      return null;
    }

    Matcher matcher = URL_PATTERN.matcher(url);
    if (!matcher.matches()) {
      return null;
    }

    String hostName = matcher.group("host");
    String portAsString = getUtf(matcher.group("port"));

    if (isNullOrEmpty(hostName)) {
      return null;
    }

    int portAsInteger = NO_PORT;
    if (!isNullOrEmpty(portAsString)) {
      try {
        portAsInteger = Integer.parseInt(portAsString.trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }

    return new HostSpec(hostName.trim(), portAsInteger);
  }

  /**
   * Convert the supplied URL to UTF string
   *
   * @param url the URL to convert
   * @return the converted URL
   */
  private static @Nullable String getUtf(@Nullable String url) {
    if (isNullOrEmpty(url)) {
      return url;
    }

    try {
      return URLDecoder.decode(url, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      return "";
    }
  }

  /**
   * Check if the supplied string is null or empty
   *
   * @param s the string to analyze
   * @return true if the supplied string is null or empty
   */
  @EnsuresNonNullIf(expression = "#1", result = false)
  public static boolean isNullOrEmpty(@Nullable String s) {
    return s == null || s.equals("");
  }
}
