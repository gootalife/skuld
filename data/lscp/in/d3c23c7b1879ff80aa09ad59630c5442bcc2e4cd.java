hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/SecretManager.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class SecretManager<T extends TokenIdentifier> {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/Token.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Token<T extends TokenIdentifier> implements Writable {
  public static final Log LOG = LogFactory.getLog(Token.class);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/TokenIdentifier.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenIdentifier implements Writable {


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/TokenInfo.java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public @interface TokenInfo {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/TokenRenewer.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenRenewer {


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/TokenSelector.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TokenSelector<T extends TokenIdentifier> {
  Token<T> selectToken(Text service,

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/package-info.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
package org.apache.hadoop.security.token;
import org.apache.hadoop.classification.InterfaceAudience;

