hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/Token.java
  public Token(byte[] identifier, byte[] password, Text kind, Text service) {
    this.identifier = (identifier == null)? new byte[0] : identifier;
    this.password = (password == null)? new byte[0] : password;
    this.kind = (kind == null)? new Text() : kind;
    this.service = (service == null)? new Text() : service;
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/token/delegation/TestDelegationToken.java
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
    Assert.assertEquals(key1, key2);
    Assert.assertFalse(key2.equals(key3));
  }

  @Test
  public void testEmptyToken() throws IOException {
    Token<?> token1 = new Token<TokenIdentifier>();

    Token<?> token2 = new Token<TokenIdentifier>(new byte[0], new byte[0],
        new Text(), new Text());
    assertEquals(token1, token2);
    assertEquals(token1.encodeToUrlString(), token2.encodeToUrlString());

    token2 = new Token<TokenIdentifier>(null, null, null, null);
    assertEquals(token1, token2);
    assertEquals(token1.encodeToUrlString(), token2.encodeToUrlString());
  }
}

