hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/crypto/key/kms/KMSClientProvider.java
    }
    HttpExceptionUtils.validateResponse(conn, expectedResponse);
    if (conn.getContentType() != null
        && conn.getContentType().trim().toLowerCase()
            .startsWith(APPLICATION_JSON_MIME)
        && klass != null) {
      ObjectMapper mapper = new ObjectMapper();
      InputStream is = null;

