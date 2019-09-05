hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Shell.java
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
        throw new ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (InterruptedException ie) {
      InterruptedIOException iie = new InterruptedIOException(ie.toString());
      iie.initCause(ie);
      throw iie;
    } finally {
      if (timeOutTimer != null) {
        timeOutTimer.cancel();

