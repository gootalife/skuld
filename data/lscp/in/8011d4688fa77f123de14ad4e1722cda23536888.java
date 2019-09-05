hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Server.java
    }
    
    private void doSaslReply(Message message) throws IOException {
      setupResponse(saslResponse, saslCall,
          RpcStatusProto.SUCCESS, null,
          new RpcResponseWrapper(message), null, null);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/SaslRpcClient.java
      }
      RpcSaslProto saslMessage =
          RpcSaslProto.parseFrom(responseWrapper.getMessageBytes());
      RpcSaslProto.Builder response = null;
      switch (saslMessage.getState()) {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/UserGroupInformation.java
        .getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket ticket : tickets) {
      if (SecurityUtil.isOriginalTGT(ticket)) {
        return ticket;
      }
    }

