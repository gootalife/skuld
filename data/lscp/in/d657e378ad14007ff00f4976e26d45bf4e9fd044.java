hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/java/org/apache/hadoop/fs/http/server/HttpFSAuthenticationFilter.java
import org.apache.commons.io.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
    } catch (IOException ex) {
      throw new RuntimeException("Could not read HttpFS signature secret file: " + signatureSecretFile);
    }
    setAuthHandlerClass(props);
    props.setProperty(KerberosDelegationTokenAuthenticationHandler.TOKEN_KIND,
        WebHdfsConstants.WEBHDFS_TOKEN_KIND.toString());
    return props;
  }


