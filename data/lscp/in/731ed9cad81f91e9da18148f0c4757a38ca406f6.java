hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/net/ServerSocketUtil.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/net/ServerSocketUtil.java

package org.apache.hadoop.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServerSocketUtil {

  private static final Log LOG = LogFactory.getLog(ServerSocketUtil.class);

  public static int getPort(int port, int retries) throws IOException {
    Random rand = new Random();
    int tryPort = port;
    int tries = 0;
    while (true) {
      if (tries > 0) {
        tryPort = port + rand.nextInt(65535 - port);
      }
      LOG.info("Using port " + tryPort);
      try (ServerSocket s = new ServerSocket(tryPort)) {
        return tryPort;
      } catch (IOException e) {
        tries++;
        if (tries >= retries) {
          LOG.info("Port is already in use; giving up");
          throw e;
        } else {
          LOG.info("Port is already in use; trying again");
        }
      }
    }
  }

}

