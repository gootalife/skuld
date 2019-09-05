hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSUtilClient.java
      if (address != null) {
        InetSocketAddress isa = NetUtils.createSocketAddr(address);
        if (isa.isUnresolved()) {
          LOG.warn("Namenode for {} remains unresolved for ID {}. Check your "
              + "hdfs-site.xml file to ensure namenodes are configured "
              + "properly.", nsId, nnId);
        }
        ret.put(nnId, isa);
      }

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/BlockStoragePolicy.java
    diff(storageTypes, excess, null);
    if (storageTypes.size() < expectedSize) {
      LOG.warn("Failed to place enough replicas: expected size is {}"
          + " but only {} storage types can be selected (replication={},"
          + " selected={}, unavailable={}" + ", removed={}" + ", policy={}"
          + ")", expectedSize, storageTypes.size(), replication, storageTypes,
          unavailables, removed, this);
    }
    return storageTypes;
  }

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/CachePoolInfo.java

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.InvalidRequestException;
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolInfo {


hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/TokenAspect.java
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

  private final DTSelecorByKind dtSelector;
  private final T fs;
  private boolean hasInitedToken;
  private final Logger LOG;
  private final Text serviceName;

  TokenAspect(T fs, final Text serviceName, final Text kind) {
    this.LOG = LoggerFactory.getLogger(fs.getClass());
    this.fs = fs;
    this.dtSelector = new DTSelecorByKind(kind);
    this.serviceName = serviceName;
        fs.setDelegationToken(token);
        addRenewAction(fs);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Created new DT for {}", token.getService());
        }
      }
      hasInitedToken = true;
    Token<?> token = selectDelegationToken(ugi);
    if (token != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found existing DT for {}", token.getService());
      }
      fs.setDelegationToken(token);
      hasInitedToken = true;

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/URLConnectionFactory.java
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.LimitedPrivate({ "HDFS" })
@InterfaceStability.Unstable
public class URLConnectionFactory {
  private static final Logger LOG = LoggerFactory
      .getLogger(URLConnectionFactory.class);

      throws IOException, AuthenticationException {
    if (isSpnego) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("open AuthenticatedURL connection {}", url);
      }
      UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
      final AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
      return new AuthenticatedURL(new KerberosUgiAuthenticator(),
          connConfigurator).openConnection(url, authToken);
    } else {
      LOG.debug("open URL connection");
      URLConnection connection = url.openConnection();
      if (connection instanceof HttpURLConnection) {
        connConfigurator.configure((HttpURLConnection) connection);

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/WebHdfsFileSystem.java

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
public class WebHdfsFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {
  public static final Logger LOG = LoggerFactory
      .getLogger(WebHdfsFileSystem.class);
  public static final int VERSION = 1;
      if (token != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Using UGI token: {}", token);
        }
        canRefreshDelegationToken = false;
      } else {
        token = getDelegationToken(null);
        if (token != null) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Fetched new token: {}", token);
          }
        } else { // security is disabled
          canRefreshDelegationToken = false;
    if (canRefreshDelegationToken) {
      Token<?> token = getDelegationToken(null);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Replaced expired token: {}", token);
      }
      setDelegationToken(token);
      replaced = (token != null);
    final URL url = new URL(getTransportScheme(), nnAddr.getHostName(),
          nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url={}", url);
    }
    return url;
  }
        + Param.toSortedString("&", parameters);
    final URL url = getNamenodeURL(path, query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url={}", url);
    }
    return url;
  }
              a.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

          if (isRetry || isFailoverAndRetry) {
            LOG.info("Retrying connect to namenode: {}. Already tried {}"
                + " time(s); retry policy is {}, delay {}ms.", nnAddr, retry,
                retryPolicy, a.delayMillis);

            if (isFailoverAndRetry) {
              resetStateToFailOver();
        final IOException ioe =
            new IOException("Response decoding failure: "+e.toString(), e);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Response decoding failure: {}", e.toString(), e);
        }
        throw ioe;
      } finally {
      }
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Token cancel failed: ", ioe);
      }
    } finally {
      super.close();

