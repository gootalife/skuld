hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3native/NativeS3FileSystem.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
            key);
        LOG.debug("{}", e, e);
        try {
          reopen(pos);
          result = in.read();
        } catch (EOFException eof) {
          LOG.debug("EOF on input stream read: {}", eof, eof);
      } catch (IOException e) {
        LOG.info( "Received IOException while reading '{}'," +
                  " attempting to reopen.", key);
        reopen(pos);
        result = in.read(b, off, len);
      }
      if (result > 0) {
    private void closeInnerStream() {
      IOUtils.closeStream(in);
      in = null;
    }

    private synchronized void reopen(long pos) throws IOException {
        LOG.debug("Reopening key '{}' for reading at position '{}", key, pos);
        InputStream newStream = store.retrieve(key, pos);
        updateInnerStream(newStream, pos);
    }

      }
      if (pos != newpos) {
        reopen(newpos);
      }
    }


hadoop-tools/hadoop-aws/src/test/java/org/apache/hadoop/fs/s3native/NativeS3FileSystemContractBaseTest.java
  
  public void testRetryOnIoException() throws Exception {
    class TestInputStream extends InputStream {
      boolean shouldThrow = true;
      int throwCount = 0;
      int pos = 0;
      byte[] bytes;
      boolean threwException = false;
      
      public TestInputStream() {
        bytes = new byte[256];
        for (int i = pos; i < 256; i++) {
          bytes[i] = (byte)i;
        }
      }
        shouldThrow = !shouldThrow;
        if (shouldThrow) {
          throwCount++;
          threwException = true;
          throw new IOException();
        }
        assertFalse("IOException was thrown. InputStream should be reopened", threwException);
        return pos++;
      }
      
        shouldThrow = !shouldThrow;
        if (shouldThrow) {
          throwCount++;
          threwException = true;
          throw new IOException();
        }
        assertFalse("IOException was thrown. InputStream should be reopened", threwException);
        int sizeToRead = Math.min(len, 256 - pos);
        for (int i = 0; i < sizeToRead; i++) {
          b[i] = bytes[pos + i];
        pos += sizeToRead;
        return sizeToRead;
      }

      public void reopenAt(long byteRangeStart) {
        threwException = false;
        pos = Long.valueOf(byteRangeStart).intValue();
      }

    }
    
    final TestInputStream is = new TestInputStream();
    
    class MockNativeFileSystemStore extends Jets3tNativeFileSystemStore {
      @Override
      public InputStream retrieve(String key, long byteRangeStart) throws IOException {
        is.reopenAt(byteRangeStart);
        return is;
      }
    }
    }
    
    assertEquals(143, ((TestInputStream)is).throwCount);
  }

}

