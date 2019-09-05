hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java

public interface HdfsClientConfigKeys {
  String  DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  long    DFS_BLOCK_SIZE_DEFAULT = 128*1024*1024;
  String  DFS_REPLICATION_KEY = "dfs.replication";
  short   DFS_REPLICATION_DEFAULT = 3;
  String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  static final String PREFIX = "dfs.client.";
  public interface Retry {
    static final String PREFIX = HdfsClientConfigKeys.PREFIX + "retry.";

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/AccessTimeParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/AccessTimeParam.java
package org.apache.hadoop.hdfs.web.resources;

public class AccessTimeParam extends LongParam {
  public static final String NAME = "accesstime";
  public static final String DEFAULT = "-1";

  private static final Domain DOMAIN = new Domain(NAME);

  public AccessTimeParam(final Long value) {
    super(DOMAIN, value, -1L, null);
  }

  public AccessTimeParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/AclPermissionParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/AclPermissionParam.java
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
    .DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.commons.lang.StringUtils;

public class AclPermissionParam extends StringParam {
  public static final String NAME = "aclspec";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME,
      Pattern.compile(DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));

  public AclPermissionParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT) ? null : str);
  }

  public AclPermissionParam(List<AclEntry> acl) {
    super(DOMAIN,parseAclSpec(acl).equals(DEFAULT) ? null : parseAclSpec(acl));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public List<AclEntry> getAclPermission(boolean includePermission) {
    final String v = getValue();
    return (v != null ? AclEntry.parseAclSpec(v, includePermission) : AclEntry
        .parseAclSpec(DEFAULT, includePermission));
  }

  private static String parseAclSpec(List<AclEntry> aclEntry) {
    return StringUtils.join(aclEntry, ",");
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/BlockSizeParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/BlockSizeParam.java
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;

import org.apache.hadoop.conf.Configuration;

public class BlockSizeParam extends LongParam {
  public static final String NAME = "blocksize";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME);

  public BlockSizeParam(final Long value) {
    super(DOMAIN, value, 1L, null);
  }

  public BlockSizeParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public long getValue(final Configuration conf) {
    return getValue() != null? getValue()
        : conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/BooleanParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/BooleanParam.java
package org.apache.hadoop.hdfs.web.resources;

abstract class BooleanParam extends Param<Boolean, BooleanParam.Domain> {
  static final String TRUE = "true";
  static final String FALSE = "false";

  @Override
  public String getValueString() {
    return value.toString();
  }

  BooleanParam(final Domain domain, final Boolean value) {
    super(domain, value);
  }

  static final class Domain extends Param.Domain<Boolean> {
    Domain(final String paramName) {
      super(paramName);
    }

    @Override
    public String getDomain() {
      return "<" + NULL + " | boolean>";
    }

    @Override
    Boolean parse(final String str) {
      if (TRUE.equalsIgnoreCase(str)) {
        return true;
      } else if (FALSE.equalsIgnoreCase(str)) {
        return false;
      }
      throw new IllegalArgumentException("Failed to parse \"" + str
          + "\" to Boolean.");
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ConcatSourcesParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ConcatSourcesParam.java

package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.fs.Path;

public class ConcatSourcesParam extends StringParam {
  public static final String NAME = "sources";

  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  private static String paths2String(Path[] paths) {
    if (paths == null || paths.length == 0) {
      return "";
    }
    final StringBuilder b = new StringBuilder(paths[0].toUri().getPath());
    for(int i = 1; i < paths.length; i++) {
      b.append(',').append(paths[i].toUri().getPath());
    }
    return b.toString();
  }

  public ConcatSourcesParam(String str) {
    super(DOMAIN, str);
  }

  public ConcatSourcesParam(Path[] paths) {
    this(paths2String(paths));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public final String[] getAbsolutePaths() {
    final String[] paths = getValue().split(",");
    return paths;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/CreateParentParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/CreateParentParam.java
package org.apache.hadoop.hdfs.web.resources;

public class CreateParentParam extends BooleanParam {
  public static final String NAME = "createparent";
  public static final String DEFAULT = FALSE;

  private static final Domain DOMAIN = new Domain(NAME);

  public CreateParentParam(final Boolean value) {
    super(DOMAIN, value);
  }

  public CreateParentParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DelegationParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DelegationParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.security.UserGroupInformation;

public class DelegationParam extends StringParam {
  public static final String NAME = "delegation";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public DelegationParam(final String str) {
    super(DOMAIN, UserGroupInformation.isSecurityEnabled()
        && str != null && !str.equals(DEFAULT)? str: null);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DeleteOpParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DeleteOpParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.net.HttpURLConnection;

public class DeleteOpParam extends HttpOpParam<DeleteOpParam.Op> {
  public static enum Op implements HttpOpParam.Op {
    DELETE(HttpURLConnection.HTTP_OK),
    DELETESNAPSHOT(HttpURLConnection.HTTP_OK),

    NULL(HttpURLConnection.HTTP_NOT_IMPLEMENTED);

    final int expectedHttpResponseCode;

    Op(final int expectedHttpResponseCode) {
      this.expectedHttpResponseCode = expectedHttpResponseCode;
    }

    @Override
    public HttpOpParam.Type getType() {
      return HttpOpParam.Type.DELETE;
    }

    @Override
    public boolean getRequireAuth() {
      return false;
    }

    @Override
    public boolean getDoOutput() {
      return false;
    }

    @Override
    public boolean getRedirect() {
      return false;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return expectedHttpResponseCode;
    }

    @Override
    public String toQueryString() {
      return NAME + "=" + this;
    }
  }

  private static final Domain<Op> DOMAIN = new Domain<Op>(NAME, Op.class);

  public DeleteOpParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DestinationParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DestinationParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.fs.Path;

public class DestinationParam extends StringParam {
  public static final String NAME = "destination";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  private static String validate(final String str) {
    if (str == null || str.equals(DEFAULT)) {
      return null;
    }
    if (!str.startsWith(Path.SEPARATOR)) {
      throw new IllegalArgumentException("Invalid parameter value: " + NAME
          + " = \"" + str + "\" is not an absolute path.");
    }
    return new Path(str).toUri().getPath();
  }

  public DestinationParam(final String str) {
    super(DOMAIN, validate(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DoAsParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/DoAsParam.java
package org.apache.hadoop.hdfs.web.resources;

public class DoAsParam extends StringParam {
  public static final String NAME = "doas";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public DoAsParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/EnumParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/EnumParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.util.Arrays;
import org.apache.hadoop.util.StringUtils;

abstract class EnumParam<E extends Enum<E>> extends Param<E, EnumParam.Domain<E>> {
  EnumParam(final Domain<E> domain, final E value) {
    super(domain, value);
  }

  static final class Domain<E extends Enum<E>> extends Param.Domain<E> {
    private final Class<E> enumClass;

    Domain(String name, final Class<E> enumClass) {
      super(name);
      this.enumClass = enumClass;
    }

    @Override
    public final String getDomain() {
      return Arrays.asList(enumClass.getEnumConstants()).toString();
    }

    @Override
    final E parse(final String str) {
      return Enum.valueOf(enumClass, StringUtils.toUpperCase(str));
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/EnumSetParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/EnumSetParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import org.apache.hadoop.util.StringUtils;

abstract class EnumSetParam<E extends Enum<E>> extends Param<EnumSet<E>, EnumSetParam.Domain<E>> {
  static <E extends Enum<E>> String toString(EnumSet<E> set) {
    if (set == null || set.isEmpty()) {
      return "";
    } else {
      final StringBuilder b = new StringBuilder();
      final Iterator<E> i = set.iterator();
      b.append(i.next());
      for(; i.hasNext(); ) {
        b.append(',').append(i.next());
      }
      return b.toString();
    }
  }

  static <E extends Enum<E>> EnumSet<E> toEnumSet(final Class<E> clazz,
      final E... values) {
    final EnumSet<E> set = EnumSet.noneOf(clazz);
    set.addAll(Arrays.asList(values));
    return set;
  }

  EnumSetParam(final Domain<E> domain, final EnumSet<E> value) {
    super(domain, value);
  }

  @Override
  public String toString() {
    return getName() + "=" + toString(value);
  }

  @Override
  public String getValueString() {
    return toString(value);
  }

  static final class Domain<E extends Enum<E>> extends Param.Domain<EnumSet<E>> {
    private final Class<E> enumClass;

    Domain(String name, final Class<E> enumClass) {
      super(name);
      this.enumClass = enumClass;
    }

    @Override
    public final String getDomain() {
      return Arrays.asList(enumClass.getEnumConstants()).toString();
    }

    @Override
    final EnumSet<E> parse(final String str) {
      final EnumSet<E> set = EnumSet.noneOf(enumClass);
      if (!str.isEmpty()) {
        for(int i, j = 0; j >= 0; ) {
          i = j > 0 ? j + 1 : 0;
          j = str.indexOf(',', i);
          final String sub = j >= 0? str.substring(i, j): str.substring(i);
          set.add(Enum.valueOf(enumClass, StringUtils.toUpperCase(sub.trim())));
        }
      }
      return set;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ExcludeDatanodesParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ExcludeDatanodesParam.java
package org.apache.hadoop.hdfs.web.resources;


public class ExcludeDatanodesParam extends StringParam {
  public static final String NAME = "excludedatanodes";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public ExcludeDatanodesParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/FsActionParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/FsActionParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.fs.permission.FsAction;

import java.util.regex.Pattern;

public class FsActionParam extends StringParam {

  public static final String NAME = "fsaction";

  public static final String DEFAULT = NULL;

  private static String FS_ACTION_PATTERN = "[rwx-]{3}";

  private static final Domain DOMAIN = new Domain(NAME,
      Pattern.compile(FS_ACTION_PATTERN));

  public FsActionParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  public FsActionParam(final FsAction value) {
    super(DOMAIN, value == null? null: value.SYMBOL);
  }

  @Override
  public String getName() {
    return NAME;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/GetOpParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/GetOpParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.net.HttpURLConnection;

public class GetOpParam extends HttpOpParam<GetOpParam.Op> {
  public static enum Op implements HttpOpParam.Op {
    OPEN(true, HttpURLConnection.HTTP_OK),

    GETFILESTATUS(false, HttpURLConnection.HTTP_OK),
    LISTSTATUS(false, HttpURLConnection.HTTP_OK),
    GETCONTENTSUMMARY(false, HttpURLConnection.HTTP_OK),
    GETFILECHECKSUM(true, HttpURLConnection.HTTP_OK),

    GETHOMEDIRECTORY(false, HttpURLConnection.HTTP_OK),
    GETDELEGATIONTOKEN(false, HttpURLConnection.HTTP_OK, true),

    GET_BLOCK_LOCATIONS(false, HttpURLConnection.HTTP_OK),
    GETACLSTATUS(false, HttpURLConnection.HTTP_OK),
    GETXATTRS(false, HttpURLConnection.HTTP_OK),
    LISTXATTRS(false, HttpURLConnection.HTTP_OK),

    NULL(false, HttpURLConnection.HTTP_NOT_IMPLEMENTED),

    CHECKACCESS(false, HttpURLConnection.HTTP_OK);

    final boolean redirect;
    final int expectedHttpResponseCode;
    final boolean requireAuth;

    Op(final boolean redirect, final int expectedHttpResponseCode) {
      this(redirect, expectedHttpResponseCode, false);
    }

    Op(final boolean redirect, final int expectedHttpResponseCode,
       final boolean requireAuth) {
      this.redirect = redirect;
      this.expectedHttpResponseCode = expectedHttpResponseCode;
      this.requireAuth = requireAuth;
    }

    @Override
    public HttpOpParam.Type getType() {
      return HttpOpParam.Type.GET;
    }

    @Override
    public boolean getRequireAuth() {
      return requireAuth;
    }

    @Override
    public boolean getDoOutput() {
      return false;
    }

    @Override
    public boolean getRedirect() {
      return redirect;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return expectedHttpResponseCode;
    }

    @Override
    public String toQueryString() {
      return NAME + "=" + this;
    }
  }

  private static final Domain<Op> DOMAIN = new Domain<Op>(NAME, Op.class);

  public GetOpParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/GroupParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/GroupParam.java
package org.apache.hadoop.hdfs.web.resources;

public class GroupParam extends StringParam {
  public static final String NAME = "group";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public GroupParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/HttpOpParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/HttpOpParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.Response;


public abstract class HttpOpParam<E extends Enum<E> & HttpOpParam.Op>
    extends EnumParam<E> {
  public static final String NAME = "op";

  public static final String DEFAULT = NULL;

  public static enum Type {
    GET, PUT, POST, DELETE;
  }

  public static interface Op {
    public Type getType();

    public boolean getRequireAuth();

    public boolean getDoOutput();

    public boolean getRedirect();

    public int getExpectedHttpResponseCode();

    public String toQueryString();
  }

  public static class TemporaryRedirectOp implements Op {
    static final TemporaryRedirectOp CREATE = new TemporaryRedirectOp(
        PutOpParam.Op.CREATE);
    static final TemporaryRedirectOp APPEND = new TemporaryRedirectOp(
        PostOpParam.Op.APPEND);
    static final TemporaryRedirectOp OPEN = new TemporaryRedirectOp(
        GetOpParam.Op.OPEN);
    static final TemporaryRedirectOp GETFILECHECKSUM = new TemporaryRedirectOp(
        GetOpParam.Op.GETFILECHECKSUM);

    static final List<TemporaryRedirectOp> values
        = Collections.unmodifiableList(Arrays.asList(CREATE, APPEND, OPEN,
                                       GETFILECHECKSUM));

    public static TemporaryRedirectOp valueOf(final Op op) {
      for(TemporaryRedirectOp t : values) {
        if (op == t.op) {
          return t;
        }
      }
      throw new IllegalArgumentException(op + " not found.");
    }

    private final Op op;

    private TemporaryRedirectOp(final Op op) {
      this.op = op;
    }

    @Override
    public Type getType() {
      return op.getType();
    }

    @Override
    public boolean getRequireAuth() {
      return op.getRequireAuth();
    }

    @Override
    public boolean getDoOutput() {
      return false;
    }

    @Override
    public boolean getRedirect() {
      return false;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return Response.Status.TEMPORARY_REDIRECT.getStatusCode();
    }

    @Override
    public String toQueryString() {
      return op.toQueryString();
    }
  }

  @Override
  public String getValueString() {
    return value.toString();
  }

  HttpOpParam(final Domain<E> domain, final E value) {
    super(domain, value);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/IntegerParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/IntegerParam.java
package org.apache.hadoop.hdfs.web.resources;

abstract class IntegerParam extends Param<Integer, IntegerParam.Domain> {
  IntegerParam(final Domain domain, final Integer value,
      final Integer min, final Integer max) {
    super(domain, value);
    checkRange(min, max);
  }

  private void checkRange(final Integer min, final Integer max) {
    if (value == null) {
      return;
    }
    if (min != null && value < min) {
      throw new IllegalArgumentException("Invalid parameter range: " + getName()
          + " = " + domain.toString(value) + " < " + domain.toString(min));
    }
    if (max != null && value > max) {
      throw new IllegalArgumentException("Invalid parameter range: " + getName()
          + " = " + domain.toString(value) + " > " + domain.toString(max));
    }
  }

  @Override
  public String toString() {
    return getName() + "=" + domain.toString(getValue());
  }

  @Override
  public String getValueString() {
    return domain.toString(getValue());
  }

  static final class Domain extends Param.Domain<Integer> {
    final int radix;

    Domain(final String paramName) {
      this(paramName, 10);
    }

    Domain(final String paramName, final int radix) {
      super(paramName);
      this.radix = radix;
    }

    @Override
    public String getDomain() {
      return "<" + NULL + " | int in radix " + radix + ">";
    }

    @Override
    Integer parse(final String str) {
      try{
        return NULL.equals(str) || str == null ? null : Integer.parseInt(str,
          radix);
      } catch(NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as a radix-" + radix + " integer.", e);
      }
    }

    String toString(final Integer n) {
      return n == null? NULL: Integer.toString(n, radix);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/LengthParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/LengthParam.java
package org.apache.hadoop.hdfs.web.resources;

public class LengthParam extends LongParam {
  public static final String NAME = "length";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME);

  public LengthParam(final Long value) {
    super(DOMAIN, value, 0L, null);
  }

  public LengthParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public long getLength() {
    Long v = getValue();
    return v == null ? -1 : v;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/LongParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/LongParam.java
package org.apache.hadoop.hdfs.web.resources;

abstract class LongParam extends Param<Long, LongParam.Domain> {
  LongParam(final Domain domain, final Long value, final Long min, final Long max) {
    super(domain, value);
    checkRange(min, max);
  }

  private void checkRange(final Long min, final Long max) {
    if (value == null) {
      return;
    }
    if (min != null && value < min) {
      throw new IllegalArgumentException("Invalid parameter range: " + getName()
          + " = " + domain.toString(value) + " < " + domain.toString(min));
    }
    if (max != null && value > max) {
      throw new IllegalArgumentException("Invalid parameter range: " + getName()
          + " = " + domain.toString(value) + " > " + domain.toString(max));
    }
  }

  @Override
  public String toString() {
    return getName() + "=" + domain.toString(getValue());
  }

  @Override
  public String getValueString() {
    return domain.toString(getValue());
  }

  static final class Domain extends Param.Domain<Long> {
    final int radix;

    Domain(final String paramName) {
      this(paramName, 10);
    }

    Domain(final String paramName, final int radix) {
      super(paramName);
      this.radix = radix;
    }

    @Override
    public String getDomain() {
      return "<" + NULL + " | long in radix " + radix + ">";
    }

    @Override
    Long parse(final String str) {
      try {
        return NULL.equals(str) || str == null ? null: Long.parseLong(str,
          radix);
      } catch(NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as a radix-" + radix + " long integer.", e);
      }
    }

    String toString(final Long n) {
      return n == null? NULL: Long.toString(n, radix);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ModificationTimeParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ModificationTimeParam.java
package org.apache.hadoop.hdfs.web.resources;

public class ModificationTimeParam extends LongParam {
  public static final String NAME = "modificationtime";
  public static final String DEFAULT = "-1";

  private static final Domain DOMAIN = new Domain(NAME);

  public ModificationTimeParam(final Long value) {
    super(DOMAIN, value, -1L, null);
  }

  public ModificationTimeParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/NewLengthParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/NewLengthParam.java
package org.apache.hadoop.hdfs.web.resources;

public class NewLengthParam extends LongParam {
  public static final String NAME = "newlength";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME);

  public NewLengthParam(final Long value) {
    super(DOMAIN, value, 0L, null);
  }

  public NewLengthParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OffsetParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OffsetParam.java
package org.apache.hadoop.hdfs.web.resources;

public class OffsetParam extends LongParam {
  public static final String NAME = "offset";
  public static final String DEFAULT = "0";

  private static final Domain DOMAIN = new Domain(NAME);

  public OffsetParam(final Long value) {
    super(DOMAIN, value, 0L, null);
  }

  public OffsetParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public Long getOffset() {
    Long offset = getValue();
    return (offset == null) ? Long.valueOf(0) : offset;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OldSnapshotNameParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OldSnapshotNameParam.java
package org.apache.hadoop.hdfs.web.resources;

public class OldSnapshotNameParam extends StringParam {
  public static final String NAME = "oldsnapshotname";

  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public OldSnapshotNameParam(final String str) {
    super(DOMAIN, str != null && !str.equals(DEFAULT) ? str : null);
  }

  @Override
  public String getName() {
    return NAME;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OverwriteParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OverwriteParam.java
package org.apache.hadoop.hdfs.web.resources;

public class OverwriteParam extends BooleanParam {
  public static final String NAME = "overwrite";
  public static final String DEFAULT = FALSE;

  private static final Domain DOMAIN = new Domain(NAME);

  public OverwriteParam(final Boolean value) {
    super(DOMAIN, value);
  }

  public OverwriteParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OwnerParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/OwnerParam.java
package org.apache.hadoop.hdfs.web.resources;

public class OwnerParam extends StringParam {
  public static final String NAME = "owner";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public OwnerParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/Param.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/Param.java
package org.apache.hadoop.hdfs.web.resources;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Comparator;


public abstract class Param<T, D extends Param.Domain<T>> {
  static final String NULL = "null";

  static final Comparator<Param<?,?>> NAME_CMP = new Comparator<Param<?,?>>() {
    @Override
    public int compare(Param<?, ?> left, Param<?, ?> right) {
      return left.getName().compareTo(right.getName());
    }
  };

  public static String toSortedString(final String separator,
      final Param<?, ?>... parameters) {
    Arrays.sort(parameters, NAME_CMP);
    final StringBuilder b = new StringBuilder();
    try {
      for(Param<?, ?> p : parameters) {
        if (p.getValue() != null) {
          b.append(separator).append(
              URLEncoder.encode(p.getName(), "UTF-8")
              + "="
              + URLEncoder.encode(p.getValueString(), "UTF-8"));
        }
      }
  } catch (UnsupportedEncodingException e) {
    throw new RuntimeException(e);
  }
    return b.toString();
  }

  final D domain;
  final T value;

  Param(final D domain, final T value) {
    this.domain = domain;
    this.value = value;
  }

  public final T getValue() {
    return value;
  }

  public abstract String getValueString();

  public abstract String getName();

  @Override
  public String toString() {
    return getName() + "=" + value;
  }

  static abstract class Domain<T> {
    final String paramName;

    Domain(final String paramName) {
      this.paramName = paramName;
    }

    public final String getParamName() {
      return paramName;
    }

    public abstract String getDomain();

    abstract T parse(String str);

    public final T parse(final String varName, final String str) {
      try {
        return str != null && str.trim().length() > 0 ? parse(str) : null;
      } catch(Exception e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" for the parameter " + varName
            + ".  The value must be in the domain " + getDomain(), e);
      }
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/PermissionParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/PermissionParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.fs.permission.FsPermission;

public class PermissionParam extends ShortParam {
  public static final String NAME = "permission";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME, 8);

  private static final short DEFAULT_PERMISSION = 0755;

  public static FsPermission getDefaultFsPermission() {
    return new FsPermission(DEFAULT_PERMISSION);
  }

  public PermissionParam(final FsPermission value) {
    super(DOMAIN, value == null? null: value.toShort(), null, null);
  }

  public PermissionParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str), (short)0, (short)01777);
  }

  @Override
  public String getName() {
    return NAME;
  }

  public FsPermission getFsPermission() {
    final Short v = getValue();
    return new FsPermission(v != null? v: DEFAULT_PERMISSION);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/PostOpParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/PostOpParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.net.HttpURLConnection;

public class PostOpParam extends HttpOpParam<PostOpParam.Op> {
  public static enum Op implements HttpOpParam.Op {
    APPEND(true, HttpURLConnection.HTTP_OK),

    CONCAT(false, HttpURLConnection.HTTP_OK),

    TRUNCATE(false, HttpURLConnection.HTTP_OK),

    NULL(false, HttpURLConnection.HTTP_NOT_IMPLEMENTED);

    final boolean doOutputAndRedirect;
    final int expectedHttpResponseCode;

    Op(final boolean doOutputAndRedirect, final int expectedHttpResponseCode) {
      this.doOutputAndRedirect = doOutputAndRedirect;
      this.expectedHttpResponseCode = expectedHttpResponseCode;
    }

    @Override
    public Type getType() {
      return Type.POST;
    }

    @Override
    public boolean getRequireAuth() {
      return false;
    }

    @Override
    public boolean getDoOutput() {
      return doOutputAndRedirect;
    }

    @Override
    public boolean getRedirect() {
      return doOutputAndRedirect;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return expectedHttpResponseCode;
    }

    @Override
    public String toQueryString() {
      return NAME + "=" + this;
    }
  }

  private static final Domain<Op> DOMAIN = new Domain<PostOpParam.Op>(NAME, Op.class);

  public PostOpParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/PutOpParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/PutOpParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.net.HttpURLConnection;

public class PutOpParam extends HttpOpParam<PutOpParam.Op> {
  public static enum Op implements HttpOpParam.Op {
    CREATE(true, HttpURLConnection.HTTP_CREATED),

    MKDIRS(false, HttpURLConnection.HTTP_OK),
    CREATESYMLINK(false, HttpURLConnection.HTTP_OK),
    RENAME(false, HttpURLConnection.HTTP_OK),
    SETREPLICATION(false, HttpURLConnection.HTTP_OK),

    SETOWNER(false, HttpURLConnection.HTTP_OK),
    SETPERMISSION(false, HttpURLConnection.HTTP_OK),
    SETTIMES(false, HttpURLConnection.HTTP_OK),

    RENEWDELEGATIONTOKEN(false, HttpURLConnection.HTTP_OK, true),
    CANCELDELEGATIONTOKEN(false, HttpURLConnection.HTTP_OK, true),

    MODIFYACLENTRIES(false, HttpURLConnection.HTTP_OK),
    REMOVEACLENTRIES(false, HttpURLConnection.HTTP_OK),
    REMOVEDEFAULTACL(false, HttpURLConnection.HTTP_OK),
    REMOVEACL(false, HttpURLConnection.HTTP_OK),
    SETACL(false, HttpURLConnection.HTTP_OK),

    SETXATTR(false, HttpURLConnection.HTTP_OK),
    REMOVEXATTR(false, HttpURLConnection.HTTP_OK),

    CREATESNAPSHOT(false, HttpURLConnection.HTTP_OK),
    RENAMESNAPSHOT(false, HttpURLConnection.HTTP_OK),

    NULL(false, HttpURLConnection.HTTP_NOT_IMPLEMENTED);

    final boolean doOutputAndRedirect;
    final int expectedHttpResponseCode;
    final boolean requireAuth;

    Op(final boolean doOutputAndRedirect, final int expectedHttpResponseCode) {
      this(doOutputAndRedirect, expectedHttpResponseCode, false);
    }

    Op(final boolean doOutputAndRedirect, final int expectedHttpResponseCode,
       final boolean requireAuth) {
      this.doOutputAndRedirect = doOutputAndRedirect;
      this.expectedHttpResponseCode = expectedHttpResponseCode;
      this.requireAuth = requireAuth;
    }

    @Override
    public HttpOpParam.Type getType() {
      return HttpOpParam.Type.PUT;
    }

    @Override
    public boolean getRequireAuth() {
      return requireAuth;
    }

    @Override
    public boolean getDoOutput() {
      return doOutputAndRedirect;
    }

    @Override
    public boolean getRedirect() {
      return doOutputAndRedirect;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return expectedHttpResponseCode;
    }

    @Override
    public String toQueryString() {
      return NAME + "=" + this;
    }
  }

  private static final Domain<Op> DOMAIN = new Domain<Op>(NAME, Op.class);

  public PutOpParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/RecursiveParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/RecursiveParam.java
package org.apache.hadoop.hdfs.web.resources;

public class RecursiveParam extends BooleanParam {
  public static final String NAME = "recursive";
  public static final String DEFAULT = FALSE;

  private static final Domain DOMAIN = new Domain(NAME);

  public RecursiveParam(final Boolean value) {
    super(DOMAIN, value);
  }

  public RecursiveParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/RenameOptionSetParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/RenameOptionSetParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.fs.Options;

public class RenameOptionSetParam extends EnumSetParam<Options.Rename> {
  public static final String NAME = "renameoptions";
  public static final String DEFAULT = "";

  private static final Domain<Options.Rename> DOMAIN = new Domain<Options.Rename>(
      NAME, Options.Rename.class);

  public RenameOptionSetParam(final Options.Rename... options) {
    super(DOMAIN, toEnumSet(Options.Rename.class, options));
  }

  public RenameOptionSetParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/RenewerParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/RenewerParam.java
package org.apache.hadoop.hdfs.web.resources;

public class RenewerParam extends StringParam {
  public static final String NAME = "renewer";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME, null);

  public RenewerParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ReplicationParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ReplicationParam.java
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_KEY;

import org.apache.hadoop.conf.Configuration;

public class ReplicationParam extends ShortParam {
  public static final String NAME = "replication";
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME);

  public ReplicationParam(final Short value) {
    super(DOMAIN, value, (short)1, null);
  }

  public ReplicationParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public short getValue(final Configuration conf) {
    return getValue() != null? getValue()
        : (short)conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ShortParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/ShortParam.java
package org.apache.hadoop.hdfs.web.resources;

abstract class ShortParam extends Param<Short, ShortParam.Domain> {
  ShortParam(final Domain domain, final Short value,
      final Short min, final Short max) {
    super(domain, value);
    checkRange(min, max);
  }

  private void checkRange(final Short min, final Short max) {
    if (value == null) {
      return;
    }
    if (min != null && value < min) {
      throw new IllegalArgumentException("Invalid parameter range: " + getName()
          + " = " + domain.toString(value) + " < " + domain.toString(min));
    }
    if (max != null && value > max) {
      throw new IllegalArgumentException("Invalid parameter range: " + getName()
          + " = " + domain.toString(value) + " > " + domain.toString(max));
    }
  }

  @Override
  public String toString() {
    return getName() + "=" + domain.toString(getValue());
  }

  @Override
  public final String getValueString() {
    return domain.toString(getValue());
  }

  static final class Domain extends Param.Domain<Short> {
    final int radix;

    Domain(final String paramName) {
      this(paramName, 10);
    }

    Domain(final String paramName, final int radix) {
      super(paramName);
      this.radix = radix;
    }

    @Override
    public String getDomain() {
      return "<" + NULL + " | short in radix " + radix + ">";
    }

    @Override
    Short parse(final String str) {
      try {
        return NULL.equals(str) || str == null ? null : Short.parseShort(str,
          radix);
      } catch(NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as a radix-" + radix + " short integer.", e);
      }
    }

    String toString(final Short n) {
      return n == null? NULL: Integer.toString(n, radix);
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/SnapshotNameParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/SnapshotNameParam.java
package org.apache.hadoop.hdfs.web.resources;

public class SnapshotNameParam extends StringParam {
  public static final String NAME = "snapshotname";

  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public SnapshotNameParam(final String str) {
    super(DOMAIN, str != null && !str.equals(DEFAULT) ? str : null);
  }

  @Override
  public String getName() {
    return NAME;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/StringParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/StringParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.util.regex.Pattern;

abstract class StringParam extends Param<String, StringParam.Domain> {
  StringParam(final Domain domain, String str) {
    super(domain, domain.parse(str));
  }

  @Override
  public String getValueString() {
    return value;
  }

  static final class Domain extends Param.Domain<String> {
    private final Pattern pattern;

    Domain(final String paramName, final Pattern pattern) {
      super(paramName);
      this.pattern = pattern;
    }

    @Override
    public final String getDomain() {
      return pattern == null ? "<String>" : pattern.pattern();
    }

    @Override
    final String parse(final String str) {
      if (str != null && pattern != null) {
        if (!pattern.matcher(str).matches()) {
          throw new IllegalArgumentException("Invalid value: \"" + str
              + "\" does not belong to the domain " + getDomain());
        }
      }
      return str;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/TokenArgumentParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/TokenArgumentParam.java
package org.apache.hadoop.hdfs.web.resources;

public class TokenArgumentParam extends StringParam {
  public static final String NAME = "token";
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  public TokenArgumentParam(final String str) {
    super(DOMAIN, str != null && !str.equals(DEFAULT) ? str : null);
  }

  @Override
  public String getName() {
    return NAME;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/UserParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/UserParam.java
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT;
import org.apache.hadoop.security.UserGroupInformation;
import com.google.common.annotations.VisibleForTesting;

import java.text.MessageFormat;
import java.util.regex.Pattern;

public class UserParam extends StringParam {
  public static final String NAME = "user.name";
  public static final String DEFAULT = "";

  private static Domain domain = new Domain(NAME, Pattern.compile(DFS_WEBHDFS_USER_PATTERN_DEFAULT));

  @VisibleForTesting
  public static Domain getUserPatternDomain() {
    return domain;
  }

  @VisibleForTesting
  public static void setUserPatternDomain(Domain dm) {
    domain = dm;
  }

  public static void setUserPattern(String pattern) {
    domain = new Domain(NAME, Pattern.compile(pattern));
  }

  private static String validateLength(String str) {
    if (str == null) {
      throw new IllegalArgumentException(
        MessageFormat.format("Parameter [{0}], cannot be NULL", NAME));
    }
    int len = str.length();
    if (len < 1) {
      throw new IllegalArgumentException(MessageFormat.format(
        "Parameter [{0}], it's length must be at least 1", NAME));
    }
    return str;
  }

  public UserParam(final String str) {
    super(domain, str == null || str.equals(DEFAULT)? null : validateLength(str));
  }

  public UserParam(final UserGroupInformation ugi) {
    this(ugi.getShortUserName());
  }

  @Override
  public String getName() {
    return NAME;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrEncodingParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrEncodingParam.java
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.fs.XAttrCodec;

public class XAttrEncodingParam extends EnumParam<XAttrCodec> {
  public static final String NAME = "encoding";
  public static final String DEFAULT = "";

  private static final Domain<XAttrCodec> DOMAIN =
      new Domain<XAttrCodec>(NAME, XAttrCodec.class);

  public XAttrEncodingParam(final XAttrCodec encoding) {
    super(DOMAIN, encoding);
  }

  public XAttrEncodingParam(final String str) {
    super(DOMAIN, str != null && !str.isEmpty() ? DOMAIN.parse(str) : null);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getValueString() {
    return value.toString();
  }

  public XAttrCodec getEncoding() {
    return getValue();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrNameParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrNameParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.util.regex.Pattern;

public class XAttrNameParam extends StringParam {
  public static final String NAME = "xattr.name";
  public static final String DEFAULT = "";

  private static Domain DOMAIN = new Domain(NAME,
      Pattern.compile(".*"));

  public XAttrNameParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT) ? null : str);
  }

  @Override
  public String getName() {
    return NAME;
  }

  public String getXAttrName() {
    final String v = getValue();
    return v;
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrSetFlagParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrSetFlagParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.util.EnumSet;

import org.apache.hadoop.fs.XAttrSetFlag;

public class XAttrSetFlagParam extends EnumSetParam<XAttrSetFlag> {
  public static final String NAME = "flag";
  public static final String DEFAULT = "";

  private static final Domain<XAttrSetFlag> DOMAIN = new Domain<XAttrSetFlag>(
      NAME, XAttrSetFlag.class);

  public XAttrSetFlagParam(final EnumSet<XAttrSetFlag> flag) {
    super(DOMAIN, flag);
  }

  public XAttrSetFlagParam(final String str) {
    super(DOMAIN, DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public EnumSet<XAttrSetFlag> getFlag() {
    return getValue();
  }
}

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrValueParam.java
++ b/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/web/resources/XAttrValueParam.java
package org.apache.hadoop.hdfs.web.resources;

import java.io.IOException;

import org.apache.hadoop.fs.XAttrCodec;

public class XAttrValueParam extends StringParam {
  public static final String NAME = "xattr.value";
  public static final String DEFAULT = "";

  private static Domain DOMAIN = new Domain(NAME, null);

  public XAttrValueParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT) ? null : str);
  }

  @Override
  public String getName() {
    return NAME;
  }

  public byte[] getXAttrValue() throws IOException {
    final String v = getValue();
    return XAttrCodec.decodeValue(v);
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {
  public static final String  DFS_BLOCK_SIZE_KEY =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
  public static final long    DFS_BLOCK_SIZE_DEFAULT =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  public static final String  DFS_REPLICATION_KEY =
      HdfsClientConfigKeys.DFS_REPLICATION_KEY;
  public static final short   DFS_REPLICATION_DEFAULT =
      HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;

  public static final String  DFS_STREAM_BUFFER_SIZE_KEY = "dfs.stream-buffer-size";
  public static final int     DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  DFS_BYTES_PER_CHECKSUM_KEY = "dfs.bytes-per-checksum";
  public static final String  DFS_HDFS_BLOCKS_METADATA_ENABLED = "dfs.datanode.hdfs-blocks-metadata.enabled";
  public static final boolean DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT = false;
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;

  public static final String  DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY = "dfs.datanode.restart.replica.expiration";
  public static final String  DFS_WEBHDFS_ENABLED_KEY = "dfs.webhdfs.enabled";
  public static final boolean DFS_WEBHDFS_ENABLED_DEFAULT = true;
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY = "dfs.webhdfs.user.provider.user.pattern";
  public static final String  DFS_WEBHDFS_USER_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT;
  public static final String  DFS_PERMISSIONS_ENABLED_KEY = "dfs.permissions.enabled";
  public static final boolean DFS_PERMISSIONS_ENABLED_DEFAULT = true;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_KEY = "dfs.permissions.superusergroup";

