hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/SFTPConnectionPool.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/SFTPConnectionPool.java
package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

class SFTPConnectionPool {

  public static final Log LOG = LogFactory.getLog(SFTPFileSystem.class);
  private int maxConnection;
  private int liveConnectionCount = 0;
  private HashMap<ConnectionInfo, HashSet<ChannelSftp>> idleConnections =
      new HashMap<ConnectionInfo, HashSet<ChannelSftp>>();
  private HashMap<ChannelSftp, ConnectionInfo> con2infoMap =
      new HashMap<ChannelSftp, ConnectionInfo>();

  SFTPConnectionPool(int maxConnection) {
    this.maxConnection = maxConnection;
  }

  synchronized ChannelSftp getFromPool(ConnectionInfo info) throws IOException {
    Set<ChannelSftp> cons = idleConnections.get(info);
    ChannelSftp channel;

    if (cons != null && cons.size() > 0) {
      Iterator<ChannelSftp> it = cons.iterator();
      if (it.hasNext()) {
        channel = it.next();
        idleConnections.remove(info);
        return channel;
      } else {
        throw new IOException("Connection pool error.");
      }
    }
    return null;
  }

  synchronized void returnToPool(ChannelSftp channel) {
    ConnectionInfo info = con2infoMap.get(channel);
    HashSet<ChannelSftp> cons = idleConnections.get(info);
    if (cons == null) {
      cons = new HashSet<ChannelSftp>();
      idleConnections.put(info, cons);
    }
    cons.add(channel);

  }

  synchronized void shutdown() {
    if (this.con2infoMap == null){
      return; // already shutdown in case it is called
    }
    LOG.info("Inside shutdown, con2infoMap size=" + con2infoMap.size());

    this.maxConnection = 0;
    Set<ChannelSftp> cons = con2infoMap.keySet();
    if (cons != null && cons.size() > 0) {
      Set<ChannelSftp> copy = new HashSet<ChannelSftp>(cons);
      for (ChannelSftp con : copy) {
        try {
          disconnect(con);
        } catch (IOException ioe) {
          ConnectionInfo info = con2infoMap.get(con);
          LOG.error(
              "Error encountered while closing connection to " + info.getHost(),
              ioe);
        }
      }
    }
    this.idleConnections = null;
    this.con2infoMap = null;
  }

  public synchronized int getMaxConnection() {
    return maxConnection;
  }

  public synchronized void setMaxConnection(int maxConn) {
    this.maxConnection = maxConn;
  }

  public ChannelSftp connect(String host, int port, String user,
      String password, String keyFile) throws IOException {
    ConnectionInfo info = new ConnectionInfo(host, port, user);
    ChannelSftp channel = getFromPool(info);

    if (channel != null) {
      if (channel.isConnected()) {
        return channel;
      } else {
        channel = null;
        synchronized (this) {
          --liveConnectionCount;
          con2infoMap.remove(channel);
        }
      }
    }

    JSch jsch = new JSch();
    Session session = null;
    try {
      if (user == null || user.length() == 0) {
        user = System.getProperty("user.name");
      }

      if (password == null) {
        password = "";
      }

      if (keyFile != null && keyFile.length() > 0) {
        jsch.addIdentity(keyFile);
      }

      if (port <= 0) {
        session = jsch.getSession(user, host);
      } else {
        session = jsch.getSession(user, host, port);
      }

      session.setPassword(password);

      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);

      session.connect();
      channel = (ChannelSftp) session.openChannel("sftp");
      channel.connect();

      synchronized (this) {
        con2infoMap.put(channel, info);
        liveConnectionCount++;
      }

      return channel;

    } catch (JSchException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  void disconnect(ChannelSftp channel) throws IOException {
    if (channel != null) {
      boolean closeConnection = false;
      synchronized (this) {
        if (liveConnectionCount > maxConnection) {
          --liveConnectionCount;
          con2infoMap.remove(channel);
          closeConnection = true;
        }
      }
      if (closeConnection) {
        if (channel.isConnected()) {
          try {
            Session session = channel.getSession();
            channel.disconnect();
            session.disconnect();
          } catch (JSchException e) {
            throw new IOException(StringUtils.stringifyException(e));
          }
        }

      } else {
        returnToPool(channel);
      }
    }
  }

  public int getIdleCount() {
    return this.idleConnections.size();
  }

  public int getLiveConnCount() {
    return this.liveConnectionCount;
  }

  public int getConnPoolSize() {
    return this.con2infoMap.size();
  }

  static class ConnectionInfo {
    private String host = "";
    private int port;
    private String user = "";

    ConnectionInfo(String hst, int prt, String usr) {
      this.host = hst;
      this.port = prt;
      this.user = usr;
    }

    public String getHost() {
      return host;
    }

    public void setHost(String hst) {
      this.host = hst;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int prt) {
      this.port = prt;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String usr) {
      this.user = usr;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj instanceof ConnectionInfo) {
        ConnectionInfo con = (ConnectionInfo) obj;

        boolean ret = true;
        if (this.host == null || !this.host.equalsIgnoreCase(con.host)) {
          ret = false;
        }
        if (this.port >= 0 && this.port != con.port) {
          ret = false;
        }
        if (this.user == null || !this.user.equalsIgnoreCase(con.user)) {
          ret = false;
        }
        return ret;
      } else {
        return false;
      }

    }

    @Override
    public int hashCode() {
      int hashCode = 0;
      if (host != null) {
        hashCode += host.hashCode();
      }
      hashCode += port;
      if (user != null) {
        hashCode += user.hashCode();
      }
      return hashCode;
    }

  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/SFTPFileSystem.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/SFTPFileSystem.java
package org.apache.hadoop.fs.sftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class SFTPFileSystem extends FileSystem {

  public static final Log LOG = LogFactory.getLog(SFTPFileSystem.class);

  private SFTPConnectionPool connectionPool;
  private URI uri;

  private static final int DEFAULT_SFTP_PORT = 22;
  private static final int DEFAULT_MAX_CONNECTION = 5;
  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
  public static final String FS_SFTP_USER_PREFIX = "fs.sftp.user.";
  public static final String FS_SFTP_PASSWORD_PREFIX = "fs.sftp.password.";
  public static final String FS_SFTP_HOST = "fs.sftp.host";
  public static final String FS_SFTP_HOST_PORT = "fs.sftp.host.port";
  public static final String FS_SFTP_KEYFILE = "fs.sftp.keyfile";
  public static final String FS_SFTP_CONNECTION_MAX = "fs.sftp.connection.max";
  public static final String E_SAME_DIRECTORY_ONLY =
      "only same directory renames are supported";
  public static final String E_HOST_NULL = "Invalid host specified";
  public static final String E_USER_NULL =
      "No user specified for sftp connection. Expand URI or credential file.";
  public static final String E_PATH_DIR = "Path %s is a directory.";
  public static final String E_FILE_STATUS = "Failed to get file status";
  public static final String E_FILE_NOTFOUND = "File %s does not exist.";
  public static final String E_FILE_EXIST = "File already exists: %s";
  public static final String E_CREATE_DIR =
      "create(): Mkdirs failed to create: %s";
  public static final String E_DIR_CREATE_FROMFILE =
      "Can't make directory for path %s since it is a file.";
  public static final String E_MAKE_DIR_FORPATH =
      "Can't make directory for path \"%s\" under \"%s\".";
  public static final String E_DIR_NOTEMPTY = "Directory: %s is not empty.";
  public static final String E_FILE_CHECK_FAILED = "File check failed";
  public static final String E_NOT_SUPPORTED = "Not supported";
  public static final String E_SPATH_NOTEXIST = "Source path %s does not exist";
  public static final String E_DPATH_EXIST =
      "Destination path %s already exist, cannot rename!";
  public static final String E_FAILED_GETHOME = "Failed to get home directory";
  public static final String E_FAILED_DISCONNECT = "Failed to disconnect";

  private void setConfigurationFromURI(URI uriInfo, Configuration conf)
      throws IOException {

    String host = uriInfo.getHost();
    host = (host == null) ? conf.get(FS_SFTP_HOST, null) : host;
    if (host == null) {
      throw new IOException(E_HOST_NULL);
    }
    conf.set(FS_SFTP_HOST, host);

    int port = uriInfo.getPort();
    port = (port == -1)
      ? conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT)
      : port;
    conf.setInt(FS_SFTP_HOST_PORT, port);

    String userAndPwdFromUri = uriInfo.getUserInfo();
    if (userAndPwdFromUri != null) {
      String[] userPasswdInfo = userAndPwdFromUri.split(":");
      String user = userPasswdInfo[0];
      user = URLDecoder.decode(user, "UTF-8");
      conf.set(FS_SFTP_USER_PREFIX + host, user);
      if (userPasswdInfo.length > 1) {
        conf.set(FS_SFTP_PASSWORD_PREFIX + host + "." +
            user, userPasswdInfo[1]);
      }
    }

    String user = conf.get(FS_SFTP_USER_PREFIX + host);
    if (user == null || user.equals("")) {
      throw new IllegalStateException(E_USER_NULL);
    }

    int connectionMax =
        conf.getInt(FS_SFTP_CONNECTION_MAX, DEFAULT_MAX_CONNECTION);
    connectionPool = new SFTPConnectionPool(connectionMax);
  }

  private ChannelSftp connect() throws IOException {
    Configuration conf = getConf();

    String host = conf.get(FS_SFTP_HOST, null);
    int port = conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT);
    String user = conf.get(FS_SFTP_USER_PREFIX + host, null);
    String pwd = conf.get(FS_SFTP_PASSWORD_PREFIX + host + "." + user, null);
    String keyFile = conf.get(FS_SFTP_KEYFILE, null);

    ChannelSftp channel =
        connectionPool.connect(host, port, user, pwd, keyFile);

    return channel;
  }

  private void disconnect(ChannelSftp channel) throws IOException {
    connectionPool.disconnect(channel);
  }

  private Path makeAbsolute(Path workDir, Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workDir, path);
  }

  private boolean exists(ChannelSftp channel, Path file) throws IOException {
    try {
      getFileStatus(channel, file);
      return true;
    } catch (FileNotFoundException fnfe) {
      return false;
    } catch (IOException ioe) {
      throw new IOException(E_FILE_STATUS, ioe);
    }
  }

  @SuppressWarnings("unchecked")
  private FileStatus getFileStatus(ChannelSftp client, Path file)
      throws IOException {
    FileStatus fileStat = null;
    Path workDir;
    try {
      workDir = new Path(client.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absolute = makeAbsolute(workDir, file);
    Path parentPath = absolute.getParent();
    if (parentPath == null) { // root directory
      long length = -1; // Length of root directory on server not known
      boolean isDir = true;
      int blockReplication = 1;
      long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
      long modTime = -1; // Modification time of root directory not known.
      Path root = new Path("/");
      return new FileStatus(length, isDir, blockReplication, blockSize,
          modTime,
          root.makeQualified(this.getUri(), this.getWorkingDirectory()));
    }
    String pathName = parentPath.toUri().getPath();
    Vector<LsEntry> sftpFiles;
    try {
      sftpFiles = (Vector<LsEntry>) client.ls(pathName);
    } catch (SftpException e) {
      throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
    }
    if (sftpFiles != null) {
      for (LsEntry sftpFile : sftpFiles) {
        if (sftpFile.getFilename().equals(file.getName())) {
          fileStat = getFileStatus(client, sftpFile, parentPath);
          break;
        }
      }
      if (fileStat == null) {
        throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
      }
    } else {
      throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
    }
    return fileStat;
  }

  private FileStatus getFileStatus(ChannelSftp channel, LsEntry sftpFile,
      Path parentPath) throws IOException {

    SftpATTRS attr = sftpFile.getAttrs();
    long length = attr.getSize();
    boolean isDir = attr.isDir();
    boolean isLink = attr.isLink();
    if (isLink) {
      String link = parentPath.toUri().getPath() + "/" + sftpFile.getFilename();
      try {
        link = channel.realpath(link);

        Path linkParent = new Path("/", link);

        FileStatus fstat = getFileStatus(channel, linkParent);
        isDir = fstat.isDirectory();
        length = fstat.getLen();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    int blockReplication = 1;
    long blockSize = DEFAULT_BLOCK_SIZE;
    long modTime = attr.getMTime() * 1000; // convert to milliseconds
    long accessTime = 0;
    FsPermission permission = getPermissions(sftpFile);
    String user = Integer.toString(attr.getUId());
    String group = Integer.toString(attr.getGId());
    Path filePath = new Path(parentPath, sftpFile.getFilename());

    return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
        accessTime, permission, user, group, filePath.makeQualified(
            this.getUri(), this.getWorkingDirectory()));
  }

  private FsPermission getPermissions(LsEntry sftpFile) {
    return new FsPermission((short) sftpFile.getAttrs().getPermissions());
  }

  private boolean mkdirs(ChannelSftp client, Path file, FsPermission permission)
      throws IOException {
    boolean created = true;
    Path workDir;
    try {
      workDir = new Path(client.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.getName();
    if (!exists(client, absolute)) {
      Path parent = absolute.getParent();
      created =
          (parent == null || mkdirs(client, parent, FsPermission.getDefault()));
      if (created) {
        String parentDir = parent.toUri().getPath();
        boolean succeeded = true;
        try {
          client.cd(parentDir);
          client.mkdir(pathName);
        } catch (SftpException e) {
          throw new IOException(String.format(E_MAKE_DIR_FORPATH, pathName,
              parentDir));
        }
        created = created & succeeded;
      }
    } else if (isFile(client, absolute)) {
      throw new IOException(String.format(E_DIR_CREATE_FROMFILE, absolute));
    }
    return created;
  }

  private boolean isFile(ChannelSftp channel, Path file) throws IOException {
    try {
      return !getFileStatus(channel, file).isDirectory();
    } catch (FileNotFoundException e) {
      return false; // file does not exist
    } catch (IOException ioe) {
      throw new IOException(E_FILE_CHECK_FAILED, ioe);
    }
  }

  private boolean delete(ChannelSftp channel, Path file, boolean recursive)
      throws IOException {
    Path workDir;
    try {
      workDir = new Path(channel.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.toUri().getPath();
    FileStatus fileStat = null;
    try {
      fileStat = getFileStatus(channel, absolute);
    } catch (FileNotFoundException e) {
      return false;
    }
    if (!fileStat.isDirectory()) {
      boolean status = true;
      try {
        channel.rm(pathName);
      } catch (SftpException e) {
        status = false;
      }
      return status;
    } else {
      boolean status = true;
      FileStatus[] dirEntries = listStatus(channel, absolute);
      if (dirEntries != null && dirEntries.length > 0) {
        if (!recursive) {
          throw new IOException(String.format(E_DIR_NOTEMPTY, file));
        }
        for (int i = 0; i < dirEntries.length; ++i) {
          delete(channel, new Path(absolute, dirEntries[i].getPath()),
              recursive);
        }
      }
      try {
        channel.rmdir(pathName);
      } catch (SftpException e) {
        status = false;
      }
      return status;
    }
  }

  @SuppressWarnings("unchecked")
  private FileStatus[] listStatus(ChannelSftp client, Path file)
      throws IOException {
    Path workDir;
    try {
      workDir = new Path(client.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (!fileStat.isDirectory()) {
      return new FileStatus[] {fileStat};
    }
    Vector<LsEntry> sftpFiles;
    try {
      sftpFiles = (Vector<LsEntry>) client.ls(absolute.toUri().getPath());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>();
    for (int i = 0; i < sftpFiles.size(); i++) {
      LsEntry entry = sftpFiles.get(i);
      String fname = entry.getFilename();
      if (!".".equalsIgnoreCase(fname) && !"..".equalsIgnoreCase(fname)) {
        fileStats.add(getFileStatus(client, entry, absolute));
      }
    }
    return fileStats.toArray(new FileStatus[fileStats.size()]);
  }

  private boolean rename(ChannelSftp channel, Path src, Path dst)
      throws IOException {
    Path workDir;
    try {
      workDir = new Path(channel.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absoluteSrc = makeAbsolute(workDir, src);
    Path absoluteDst = makeAbsolute(workDir, dst);

    if (!exists(channel, absoluteSrc)) {
      throw new IOException(String.format(E_SPATH_NOTEXIST, src));
    }
    if (exists(channel, absoluteDst)) {
      throw new IOException(String.format(E_DPATH_EXIST, dst));
    }
    boolean renamed = true;
    try {
      channel.cd("/");
      channel.rename(src.toUri().getPath(), dst.toUri().getPath());
    } catch (SftpException e) {
      renamed = false;
    }
    return renamed;
  }

  @Override
  public void initialize(URI uriInfo, Configuration conf) throws IOException {
    super.initialize(uriInfo, conf);

    setConfigurationFromURI(uriInfo, conf);
    setConf(conf);
    this.uri = uriInfo;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    ChannelSftp channel = connect();
    Path workDir;
    try {
      workDir = new Path(channel.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absolute = makeAbsolute(workDir, f);
    FileStatus fileStat = getFileStatus(channel, absolute);
    if (fileStat.isDirectory()) {
      disconnect(channel);
      throw new IOException(String.format(E_PATH_DIR, f));
    }
    InputStream is;
    try {
      absolute = new Path("/", channel.realpath(absolute.toUri().getPath()));

      is = channel.get(absolute.toUri().getPath());
    } catch (SftpException e) {
      throw new IOException(e);
    }

    FSDataInputStream fis =
        new FSDataInputStream(new SFTPInputStream(is, channel, statistics));
    return fis;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    final ChannelSftp client = connect();
    Path workDir;
    try {
      workDir = new Path(client.pwd());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    Path absolute = makeAbsolute(workDir, f);
    if (exists(client, f)) {
      if (overwrite) {
        delete(client, f, false);
      } else {
        disconnect(client);
        throw new IOException(String.format(E_FILE_EXIST, f));
      }
    }
    Path parent = absolute.getParent();
    if (parent == null || !mkdirs(client, parent, FsPermission.getDefault())) {
      parent = (parent == null) ? new Path("/") : parent;
      disconnect(client);
      throw new IOException(String.format(E_CREATE_DIR, parent));
    }
    OutputStream os;
    try {
      client.cd(parent.toUri().getPath());
      os = client.put(f.getName());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    FSDataOutputStream fos = new FSDataOutputStream(os, statistics) {
      @Override
      public void close() throws IOException {
        super.close();
        disconnect(client);
      }
    };

    return fos;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress)
      throws IOException {
    throw new IOException(E_NOT_SUPPORTED);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    ChannelSftp channel = connect();
    try {
      boolean success = rename(channel, src, dst);
      return success;
    } finally {
      disconnect(channel);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    ChannelSftp channel = connect();
    try {
      boolean success = delete(channel, f, recursive);
      return success;
    } finally {
      disconnect(channel);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    ChannelSftp client = connect();
    try {
      FileStatus[] stats = listStatus(client, f);
      return stats;
    } finally {
      disconnect(client);
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
  }

  @Override
  public Path getWorkingDirectory() {
    return getHomeDirectory();
  }

  @Override
  public Path getHomeDirectory() {
    ChannelSftp channel = null;
    try {
      channel = connect();
      Path homeDir = new Path(channel.pwd());
      return homeDir;
    } catch (Exception ioe) {
      return null;
    } finally {
      try {
        disconnect(channel);
      } catch (IOException ioe) {
        return null;
      }
    }
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    ChannelSftp client = connect();
    try {
      boolean success = mkdirs(client, f, permission);
      return success;
    } finally {
      disconnect(client);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    ChannelSftp channel = connect();
    try {
      FileStatus status = getFileStatus(channel, f);
      return status;
    } finally {
      disconnect(channel);
    }
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/SFTPInputStream.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/SFTPInputStream.java
package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.StringUtils;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

class SFTPInputStream extends FSInputStream {

  public static final String E_SEEK_NOTSUPPORTED = "Seek not supported";
  public static final String E_CLIENT_NULL =
      "SFTP client null or not connected";
  public static final String E_NULL_INPUTSTREAM = "Null InputStream";
  public static final String E_STREAM_CLOSED = "Stream closed";
  public static final String E_CLIENT_NOTCONNECTED = "Client not connected";

  private InputStream wrappedStream;
  private ChannelSftp channel;
  private FileSystem.Statistics stats;
  private boolean closed;
  private long pos;

  SFTPInputStream(InputStream stream, ChannelSftp channel,
      FileSystem.Statistics stats) {

    if (stream == null) {
      throw new IllegalArgumentException(E_NULL_INPUTSTREAM);
    }
    if (channel == null || !channel.isConnected()) {
      throw new IllegalArgumentException(E_CLIENT_NULL);
    }
    this.wrappedStream = stream;
    this.channel = channel;
    this.stats = stats;

    this.pos = 0;
    this.closed = false;
  }

  @Override
  public void seek(long position) throws IOException {
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException(E_STREAM_CLOSED);
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null & byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    if (closed) {
      throw new IOException(E_STREAM_CLOSED);
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null & result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    super.close();
    closed = true;
    if (!channel.isConnected()) {
      throw new IOException(E_CLIENT_NOTCONNECTED);
    }

    try {
      Session session = channel.getSession();
      channel.disconnect();
      session.disconnect();
    } catch (JSchException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/package-info.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/sftp/package-info.java
package org.apache.hadoop.fs.sftp;
\No newline at end of file

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/sftp/TestSFTPFileSystem.java
++ b/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/sftp/TestSFTPFileSystem.java
package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.UserAuth;
import org.apache.sshd.server.auth.UserAuthPassword;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.sftp.SftpSubsystem;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class TestSFTPFileSystem {

  private static final String TEST_SFTP_DIR = "testsftp";
  private static final String TEST_ROOT_DIR =
    System.getProperty("test.build.data", "build/test/data");

  @Rule public TestName name = new TestName();

  private static final String connection = "sftp://user:password@localhost";
  private static Path localDir = null;
  private static FileSystem localFs = null;
  private static FileSystem sftpFs = null;
  private static SshServer sshd = null;
  private static int port;

  private static void startSshdServer() throws IOException {
    sshd = SshServer.setUpDefaultServer();
    sshd.setPort(0);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());

    List<NamedFactory<UserAuth>> userAuthFactories =
        new ArrayList<NamedFactory<UserAuth>>();
    userAuthFactories.add(new UserAuthPassword.Factory());

    sshd.setUserAuthFactories(userAuthFactories);

    sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
      @Override
      public boolean authenticate(String username, String password,
          ServerSession session) {
        if (username.equals("user") && password.equals("password")) {
          return true;
        }
        return false;
      }
    });

    sshd.setSubsystemFactories(
        Arrays.<NamedFactory<Command>>asList(new SftpSubsystem.Factory()));

    sshd.start();
    port = sshd.getPort();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    assumeTrue(!Shell.WINDOWS);

    startSshdServer();

    Configuration conf = new Configuration();
    conf.setClass("fs.sftp.impl", SFTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.sftp.host.port", port);
    conf.setBoolean("fs.sftp.impl.disable.cache", true);

    localFs = FileSystem.getLocal(conf);
    localDir = localFs.makeQualified(new Path(TEST_ROOT_DIR, TEST_SFTP_DIR));
    if (localFs.exists(localDir)) {
      localFs.delete(localDir, true);
    }
    localFs.mkdirs(localDir);

    sftpFs = FileSystem.get(URI.create(connection), conf);
  }

  @AfterClass
  public static void tearDown() {
    if (localFs != null) {
      try {
        localFs.delete(localDir, true);
        localFs.close();
      } catch (IOException e) {
      }
    }
    if (sftpFs != null) {
      try {
        sftpFs.close();
      } catch (IOException e) {
      }
    }
    if (sshd != null) {
      try {
        sshd.stop(true);
      } catch (InterruptedException e) {
      }
    }
  }

  private static final Path touch(FileSystem fs, String filename)
      throws IOException {
    return touch(fs, filename, null);
  }

  private static final Path touch(FileSystem fs, String filename, byte[] data)
      throws IOException {
    Path lPath = new Path(localDir.toUri().getPath(), filename);
    FSDataOutputStream out = null;
    try {
      out = fs.create(lPath);
      if (data != null) {
        out.write(data);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return lPath;
  }

  @Test
  public void testCreateFile() throws Exception {
    Path file = touch(sftpFs, name.getMethodName().toLowerCase());
    assertTrue(localFs.exists(file));
    assertTrue(sftpFs.delete(file, false));
    assertFalse(localFs.exists(file));
  }

  @Test
  public void testFileExists() throws Exception {
    Path file = touch(localFs, name.getMethodName().toLowerCase());
    assertTrue(sftpFs.exists(file));
    assertTrue(localFs.exists(file));
    assertTrue(sftpFs.delete(file, false));
    assertFalse(sftpFs.exists(file));
    assertFalse(localFs.exists(file));
  }

  @Test
  public void testReadFile() throws Exception {
    byte[] data = "yaks".getBytes();
    Path file = touch(localFs, name.getMethodName().toLowerCase(), data);
    FSDataInputStream is = null;
    try {
      is = sftpFs.open(file);
      byte[] b = new byte[data.length];
      is.read(b);
      assertArrayEquals(data, b);
    } finally {
      if (is != null) {
        is.close();
      }
    }
    assertTrue(sftpFs.delete(file, false));
  }

  @Test
  public void testStatFile() throws Exception {
    byte[] data = "yaks".getBytes();
    Path file = touch(localFs, name.getMethodName().toLowerCase(), data);

    FileStatus lstat = localFs.getFileStatus(file);
    FileStatus sstat = sftpFs.getFileStatus(file);
    assertNotNull(sstat);

    assertEquals(lstat.getPath().toUri().getPath(),
                 sstat.getPath().toUri().getPath());
    assertEquals(data.length, sstat.getLen());
    assertEquals(lstat.getLen(), sstat.getLen());
    assertTrue(sftpFs.delete(file, false));
  }

  @Test(expected=java.io.IOException.class)
  public void testDeleteNonEmptyDir() throws Exception {
    Path file = touch(localFs, name.getMethodName().toLowerCase());
    sftpFs.delete(localDir, false);
  }

  @Test
  public void testDeleteNonExistFile() throws Exception {
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    assertFalse(sftpFs.delete(file, false));
  }

  @Test
  public void testRenameFile() throws Exception {
    byte[] data = "dingos".getBytes();
    Path file1 = touch(localFs, name.getMethodName().toLowerCase() + "1");
    Path file2 = new Path(localDir, name.getMethodName().toLowerCase() + "2");

    assertTrue(sftpFs.rename(file1, file2));

    assertTrue(sftpFs.exists(file2));
    assertFalse(sftpFs.exists(file1));

    assertTrue(localFs.exists(file2));
    assertFalse(localFs.exists(file1));

    assertTrue(sftpFs.delete(file2, false));
  }

  @Test(expected=java.io.IOException.class)
  public void testRenameNonExistFile() throws Exception {
    Path file1 = new Path(localDir, name.getMethodName().toLowerCase() + "1");
    Path file2 = new Path(localDir, name.getMethodName().toLowerCase() + "2");
    sftpFs.rename(file1, file2);
  }

  @Test(expected=java.io.IOException.class)
  public void testRenamingFileOntoExistingFile() throws Exception {
    Path file1 = touch(localFs, name.getMethodName().toLowerCase() + "1");
    Path file2 = touch(localFs, name.getMethodName().toLowerCase() + "2");
    sftpFs.rename(file1, file2);
  }

}

