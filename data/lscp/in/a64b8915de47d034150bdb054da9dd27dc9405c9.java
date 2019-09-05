hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/LogsCLI.java
package org.apache.hadoop.yarn.client.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

@Public
@Evolving
  private static final String APPLICATION_ID_OPTION = "applicationId";
  private static final String NODE_ADDRESS_OPTION = "nodeAddress";
  private static final String APP_OWNER_OPTION = "appOwner";
  private static final String AM_CONTAINER_OPTION = "am";
  private static final String CONTAINER_LOG_FILES = "logFiles";
  public static final String HELP_CMD = "help";

  @Override
        new Option(APPLICATION_ID_OPTION, true, "ApplicationId (required)");
    appIdOpt.setRequired(true);
    opts.addOption(appIdOpt);
    opts.addOption(CONTAINER_ID_OPTION, true, "ContainerId. "
        + "By default, it will only print syslog if the application is runing."
        + " Work with -logFiles to get other logs.");
    opts.addOption(NODE_ADDRESS_OPTION, true, "NodeAddress in the format "
      + "nodename:port");
    opts.addOption(APP_OWNER_OPTION, true,
      "AppOwner (assumed to be current user if not specified)");
    Option amOption = new Option(AM_CONTAINER_OPTION, true, 
      "Prints the AM Container logs for this application. "
      + "Specify comma-separated value to get logs for related AM Container. "
      + "For example, If we specify -am 1,2, we will get the logs for "
      + "the first AM Container as well as the second AM Container. "
      + "To get logs for all AM Containers, use -am ALL. "
      + "To get logs for the latest AM Container, use -am -1. "
      + "By default, it will only print out syslog. Work with -logFiles "
      + "to get other logs");
    amOption.setValueSeparator(',');
    amOption.setArgs(Option.UNLIMITED_VALUES);
    amOption.setArgName("AM Containers");
    opts.addOption(amOption);
    Option logFileOpt = new Option(CONTAINER_LOG_FILES, true,
      "Work with -am/-containerId and specify comma-separated value "
      + "to get specified Container log files");
    logFileOpt.setValueSeparator(',');
    logFileOpt.setArgs(Option.UNLIMITED_VALUES);
    logFileOpt.setArgName("Log File Name");
    opts.addOption(logFileOpt);

    opts.getOption(APPLICATION_ID_OPTION).setArgName("Application ID");
    opts.getOption(CONTAINER_ID_OPTION).setArgName("Container ID");
    opts.getOption(NODE_ADDRESS_OPTION).setArgName("Node Address");
    opts.getOption(APP_OWNER_OPTION).setArgName("Application Owner");
    opts.getOption(AM_CONTAINER_OPTION).setArgName("AM Containers");

    Options printOpts = new Options();
    printOpts.addOption(opts.getOption(HELP_CMD));
    printOpts.addOption(opts.getOption(CONTAINER_ID_OPTION));
    printOpts.addOption(opts.getOption(NODE_ADDRESS_OPTION));
    printOpts.addOption(opts.getOption(APP_OWNER_OPTION));
    printOpts.addOption(opts.getOption(AM_CONTAINER_OPTION));
    printOpts.addOption(opts.getOption(CONTAINER_LOG_FILES));

    if (args.length < 1) {
      printHelpMessage(printOpts);
    String containerIdStr = null;
    String nodeAddress = null;
    String appOwner = null;
    boolean getAMContainerLogs = false;
    String[] logFiles = null;
    List<String> amContainersList = new ArrayList<String>();
    try {
      CommandLine commandLine = parser.parse(opts, args, true);
      appIdStr = commandLine.getOptionValue(APPLICATION_ID_OPTION);
      containerIdStr = commandLine.getOptionValue(CONTAINER_ID_OPTION);
      nodeAddress = commandLine.getOptionValue(NODE_ADDRESS_OPTION);
      appOwner = commandLine.getOptionValue(APP_OWNER_OPTION);
      getAMContainerLogs = commandLine.hasOption(AM_CONTAINER_OPTION);
      if (getAMContainerLogs) {
        String[] amContainers = commandLine.getOptionValues(AM_CONTAINER_OPTION);
        for (String am : amContainers) {
          boolean errorInput = false;
          if (!am.trim().equalsIgnoreCase("ALL")) {
            try {
              int id = Integer.parseInt(am.trim());
              if (id != -1 && id <= 0) {
                errorInput = true;
              }
            } catch (NumberFormatException ex) {
              errorInput = true;
            }
            if (errorInput) {
              System.err.println(
                "Invalid input for option -am. Valid inputs are 'ALL', -1 "
                + "and any other integer which is larger than 0.");
              printHelpMessage(printOpts);
              return -1;
            }
            amContainersList.add(am.trim());
          } else {
            amContainersList.add("ALL");
            break;
          }
        }
      }
      if (commandLine.hasOption(CONTAINER_LOG_FILES)) {
        logFiles = commandLine.getOptionValues(CONTAINER_LOG_FILES);
      }
    } catch (ParseException e) {
      System.err.println("options parsing failed: " + e.getMessage());
      printHelpMessage(printOpts);
      return -1;
    }

    LogCLIHelpers logCliHelper = new LogCLIHelpers();
    logCliHelper.setConf(getConf());

    if (appOwner == null || appOwner.isEmpty()) {
      appOwner = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    YarnApplicationState appState = YarnApplicationState.NEW;
    try {
      appState = getApplicationState(appId);
      if (appState == YarnApplicationState.NEW
          || appState == YarnApplicationState.NEW_SAVING
          || appState == YarnApplicationState.SUBMITTED) {
        System.out.println("Logs are not avaiable right now.");
        return -1;
      }
    } catch (IOException | YarnException e) {
      System.err.println("Unable to get ApplicationState."
          + " Attempting to fetch logs directly from the filesystem.");
    }

    if (getAMContainerLogs) {
      if (logFiles == null || logFiles.length == 0) {
        logFiles = new String[] { "syslog" };
      }
      if (appState == YarnApplicationState.ACCEPTED
          || appState == YarnApplicationState.RUNNING) {
        return printAMContainerLogs(getConf(), appIdStr, amContainersList,
          logFiles, logCliHelper, appOwner, false);
      } else {
        if (getConf().getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
          return printAMContainerLogs(getConf(), appIdStr, amContainersList,
            logFiles, logCliHelper, appOwner, true);
        } else {
          System.out
            .println("Can not get AMContainers logs for the application:"
                + appId);
          System.out.println("This application:" + appId + " is finished."
              + " Please enable the application history service. Or Using "
              + "yarn logs -applicationId <appId> -containerId <containerId> "
              + "--nodeAddress <nodeHttpAddress> to get the container logs");
          return -1;
        }
      }
    }

    int resultCode = 0;
    if (containerIdStr != null) {
      if (nodeAddress != null && isApplicationFinished(appState)) {
        return logCliHelper.dumpAContainersLogsForALogType(appIdStr,
            containerIdStr, nodeAddress, appOwner, logFiles == null ? null
                : Arrays.asList(logFiles));
      }
      try {
        ContainerReport report = getContainerReport(containerIdStr);
        String nodeHttpAddress =
            report.getNodeHttpAddress().replaceFirst(
              WebAppUtils.getHttpSchemePrefix(getConf()), "");
        String nodeId = report.getAssignedNode().toString();
        if (!isApplicationFinished(appState)) {
          if (logFiles == null || logFiles.length == 0) {
            logFiles = new String[] { "syslog" };
          }
          printContainerLogsFromRunningApplication(getConf(), appIdStr,
            containerIdStr, nodeHttpAddress, nodeId, logFiles, logCliHelper,
            appOwner);
        } else {
          printContainerLogsForFinishedApplication(appIdStr, containerIdStr,
            nodeId, logFiles, logCliHelper, appOwner);
        }
        return resultCode;
      } catch (IOException | YarnException ex) {
        System.err.println("Unable to get logs for this container:"
            + containerIdStr + "for the application:" + appId);
        if (!getConf().getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
          System.out.println("Please enable the application history service. Or ");
        }
        System.out.println("Using "
            + "yarn logs -applicationId <appId> -containerId <containerId> "
            + "--nodeAddress <nodeHttpAddress> to get the container logs");
        return -1;
      }
    } else {
      if (nodeAddress == null) {
        resultCode =
            logCliHelper.dumpAllContainersLogs(appId, appOwner, System.out);
      } else {
        System.out.println("Should at least provide ContainerId!");
        printHelpMessage(printOpts);
        resultCode = -1;
      }
    }
    return resultCode;
  }

  private YarnApplicationState getApplicationState(ApplicationId appId)
      throws IOException, YarnException {
    YarnClient yarnClient = createYarnClient();

    try {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      return appReport.getYarnApplicationState();
    } finally {
      yarnClient.close();
    }
  }
  
  @VisibleForTesting
    formatter.setSyntaxPrefix("");
    formatter.printHelp("general options are:", options);
  }

  private List<JSONObject> getAMContainerInfoForRMWebService(
      Configuration conf, String appId) throws ClientHandlerException,
      UniformInterfaceException, JSONException {
    Client webServiceClient = Client.create();
    String webAppAddress =
        WebAppUtils.getWebAppBindURL(conf, YarnConfiguration.RM_BIND_HOST,
          WebAppUtils.getRMWebAppURLWithScheme(conf));
    WebResource webResource = webServiceClient.resource(webAppAddress);

    ClientResponse response =
        webResource.path("ws").path("v1").path("cluster").path("apps")
          .path(appId).path("appattempts").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    JSONObject json =
        response.getEntity(JSONObject.class).getJSONObject("appAttempts");
    JSONArray requests = json.getJSONArray("appAttempt");
    List<JSONObject> amContainersList = new ArrayList<JSONObject>();
    for (int i = 0; i < requests.length(); i++) {
      amContainersList.add(requests.getJSONObject(i));
    }
    return amContainersList;
  }

  private List<JSONObject> getAMContainerInfoForAHSWebService(Configuration conf,
      String appId) throws ClientHandlerException, UniformInterfaceException,
      JSONException {
    Client webServiceClient = Client.create();
    String webAppAddress =
        WebAppUtils.getHttpSchemePrefix(conf)
            + WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    WebResource webResource = webServiceClient.resource(webAppAddress);

    ClientResponse response =
        webResource.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId).path("appattempts").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    JSONObject json = response.getEntity(JSONObject.class);
    JSONArray requests = json.getJSONArray("appAttempt");
    List<JSONObject> amContainersList = new ArrayList<JSONObject>();
    for (int i = 0; i < requests.length(); i++) {
      amContainersList.add(requests.getJSONObject(i));
    }
    Collections.reverse(amContainersList);
    return amContainersList;
  }

  private void printContainerLogsFromRunningApplication(Configuration conf,
      String appId, String containerIdStr, String nodeHttpAddress,
      String nodeId, String[] logFiles, LogCLIHelpers logCliHelper,
      String appOwner) throws IOException {
    Client webServiceClient = Client.create();
    String containerString = "\n\nContainer: " + containerIdStr;
    System.out.println(containerString);
    System.out.println(StringUtils.repeat("=", containerString.length()));
    for (String logFile : logFiles) {
      System.out.println("LogType:" + logFile);
      System.out.println("Log Upload Time:"
          + Times.format(System.currentTimeMillis()));
      System.out.println("Log Contents:");
      try {
        WebResource webResource =
            webServiceClient.resource(WebAppUtils.getHttpSchemePrefix(conf)
                + nodeHttpAddress);
        ClientResponse response =
            webResource.path("ws").path("v1").path("node")
              .path("containerlogs").path(containerIdStr).path(logFile)
              .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
        System.out.println(response.getEntity(String.class));
        System.out.println("End of LogType:" + logFile);
      } catch (ClientHandlerException | UniformInterfaceException ex) {
        System.out.println("Can not find the log file:" + logFile
            + " for the container:" + containerIdStr + " in NodeManager:"
            + nodeId);
      }
    }
    logCliHelper.dumpAContainersLogsForALogType(appId, containerIdStr, nodeId,
      appOwner, Arrays.asList(logFiles));
  }

  private void printContainerLogsForFinishedApplication(String appId,
      String containerId, String nodeAddress, String[] logFiles,
      LogCLIHelpers logCliHelper, String appOwner) throws IOException {
    String containerString = "\n\nContainer: " + containerId;
    System.out.println(containerString);
    System.out.println(StringUtils.repeat("=", containerString.length()));
    logCliHelper.dumpAContainersLogsForALogType(appId, containerId,
      nodeAddress, appOwner, logFiles != null ? Arrays.asList(logFiles) : null);
  }

  private ContainerReport getContainerReport(String containerIdStr)
      throws YarnException, IOException {
    YarnClient yarnClient = createYarnClient();
    try {
      return yarnClient.getContainerReport(ConverterUtils
        .toContainerId(containerIdStr));
    } finally {
      yarnClient.close();
    }
  }

  private boolean isApplicationFinished(YarnApplicationState appState) {
    return appState == YarnApplicationState.FINISHED
        || appState == YarnApplicationState.FAILED
        || appState == YarnApplicationState.KILLED; 
  }

  private int printAMContainerLogs(Configuration conf, String appId,
      List<String> amContainers, String[] logFiles, LogCLIHelpers logCliHelper,
      String appOwner, boolean applicationFinished) throws Exception {
    List<JSONObject> amContainersList = null;
    List<AMLogsRequest> requests = new ArrayList<AMLogsRequest>();
    boolean getAMContainerLists = false;
    String errorMessage = "";
    try {
      amContainersList = getAMContainerInfoForRMWebService(conf, appId);
      if (amContainersList != null && !amContainersList.isEmpty()) {
        getAMContainerLists = true;
        for (JSONObject amContainer : amContainersList) {
          AMLogsRequest request = new AMLogsRequest(applicationFinished);
          request.setAmContainerId(amContainer.getString("containerId"));
          request.setNodeHttpAddress(amContainer.getString("nodeHttpAddress"));
          request.setNodeId(amContainer.getString("nodeId"));
          requests.add(request);
        }
      }
    } catch (Exception ex) {
      errorMessage = ex.getMessage();
      if (applicationFinished) {
        try {
          amContainersList = getAMContainerInfoForAHSWebService(conf, appId);
          if (amContainersList != null && !amContainersList.isEmpty()) {
            getAMContainerLists = true;
            for (JSONObject amContainer : amContainersList) {
              AMLogsRequest request = new AMLogsRequest(applicationFinished);
              request.setAmContainerId(amContainer.getString("amContainerId"));
              requests.add(request);
            }
          }
        } catch (Exception e) {
          errorMessage = e.getMessage();
        }
      }
    }

    if (!getAMContainerLists) {
      System.err.println("Unable to get AM container informations "
          + "for the application:" + appId);
      System.err.println(errorMessage);
      return -1;
    }

    if (amContainers.contains("ALL")) {
      for (AMLogsRequest request : requests) {
        outputAMContainerLogs(request, conf, appId, logFiles, logCliHelper,
          appOwner);
      }
      System.out.println();      
      System.out.println("Specified ALL for -am option. "
          + "Printed logs for all am containers.");
    } else {
      for (String amContainer : amContainers) {
        int amContainerId = Integer.parseInt(amContainer.trim());
        if (amContainerId == -1) {
          outputAMContainerLogs(requests.get(requests.size() - 1), conf, appId,
            logFiles, logCliHelper, appOwner);
        } else {
          if (amContainerId <= requests.size()) {
            outputAMContainerLogs(requests.get(amContainerId - 1), conf, appId,
              logFiles, logCliHelper, appOwner);
          }
        }
      }
    }
    return 0;
  }

  private void outputAMContainerLogs(AMLogsRequest request, Configuration conf,
      String appId, String[] logFiles, LogCLIHelpers logCliHelper,
      String appOwner) throws Exception {
    String nodeHttpAddress = request.getNodeHttpAddress();
    String containerId = request.getAmContainerId();
    String nodeId = request.getNodeId();

    if (request.isAppFinished()) {
      if (containerId != null && !containerId.isEmpty()) {
        if (nodeId == null || nodeId.isEmpty()) {
          try {
            nodeId =
                getContainerReport(containerId).getAssignedNode().toString();
          } catch (Exception ex) {
            System.err.println(ex);
            nodeId = null;
          }
        }
        if (nodeId != null && !nodeId.isEmpty()) {
          printContainerLogsForFinishedApplication(appId, containerId, nodeId,
            logFiles, logCliHelper, appOwner);
        }
      }
    } else {
      if (nodeHttpAddress != null && containerId != null
          && !nodeHttpAddress.isEmpty() && !containerId.isEmpty()) {
        printContainerLogsFromRunningApplication(conf, appId, containerId,
          nodeHttpAddress, nodeId, logFiles, logCliHelper, appOwner);
      }
    }
  }

  private static class AMLogsRequest {
    private String amContainerId;
    private String nodeId;
    private String nodeHttpAddress;
    private final boolean isAppFinished;

    AMLogsRequest(boolean isAppFinished) {
      this.isAppFinished = isAppFinished;
      this.setAmContainerId("");
      this.setNodeId("");
      this.setNodeHttpAddress("");
    }

    public String getAmContainerId() {
      return amContainerId;
    }

    public void setAmContainerId(String amContainerId) {
      this.amContainerId = amContainerId;
    }

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }

    public String getNodeHttpAddress() {
      return nodeHttpAddress;
    }

    public void setNodeHttpAddress(String nodeHttpAddress) {
      this.nodeHttpAddress = nodeHttpAddress;
    }

    public boolean isAppFinished() {
      return isAppFinished;
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestLogsCLI.java
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
    pw.println("usage: yarn logs -applicationId <application ID> [OPTIONS]");
    pw.println();
    pw.println("general options are:");
    pw.println(" -am <AM Containers>             Prints the AM Container logs for this");
    pw.println("                                 application. Specify comma-separated");
    pw.println("                                 value to get logs for related AM");
    pw.println("                                 Container. For example, If we specify -am");
    pw.println("                                 1,2, we will get the logs for the first");
    pw.println("                                 AM Container as well as the second AM");
    pw.println("                                 Container. To get logs for all AM");
    pw.println("                                 Containers, use -am ALL. To get logs for");
    pw.println("                                 the latest AM Container, use -am -1. By");
    pw.println("                                 default, it will only print out syslog.");
    pw.println("                                 Work with -logFiles to get other logs");
    pw.println(" -appOwner <Application Owner>   AppOwner (assumed to be current user if");
    pw.println("                                 not specified)");
    pw.println(" -containerId <Container ID>     ContainerId. By default, it will only");
    pw.println("                                 print syslog if the application is");
    pw.println("                                 runing. Work with -logFiles to get other");
    pw.println("                                 logs.");
    pw.println(" -help                           Displays help for all commands.");
    pw.println(" -logFiles <Log File Name>       Work with -am/-containerId and specify");
    pw.println("                                 comma-separated value to get specified");
    pw.println("                                 Container log files");
    pw.println(" -nodeAddress <Node Address>     NodeAddress in the format nodename:port");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
    ContainerId containerId0 = ContainerIdPBImpl.newContainerId(appAttemptId, 0);
    ContainerId containerId1 = ContainerIdPBImpl.newContainerId(appAttemptId, 1);
    ContainerId containerId2 = ContainerIdPBImpl.newContainerId(appAttemptId, 2);
    ContainerId containerId3 = ContainerIdPBImpl.newContainerId(appAttemptId, 3);
    NodeId nodeId = NodeId.newInstance("localhost", 1234);

    assertTrue(fs.mkdirs(appLogsDir));
    List<String> rootLogDirs = Arrays.asList(rootLogDir);

    List<String> logTypes = new ArrayList<String>();
    logTypes.add("syslog");
    createContainerLogInLocalDir(appLogsDir, containerId1, fs, logTypes);
    createContainerLogInLocalDir(appLogsDir, containerId2, fs, logTypes);

    logTypes.add("stdout");
    createContainerLogInLocalDir(appLogsDir, containerId3, fs, logTypes);

    Path path =
        new Path(remoteLogRootDir + ugi.getShortUserName()
      containerId1, path, fs);
    uploadContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId,
      containerId2, path, fs);
    uploadContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId,
      containerId3, path, fs);

    YarnClient mockYarnClient =
        createMockYarnClient(YarnApplicationState.FINISHED);
    int exitCode = cli.run(new String[] { "-applicationId", appId.toString() });
    assertTrue(exitCode == 0);
    assertTrue(sysOutStream.toString().contains(
      "Hello container_0_0001_01_000001 in syslog!"));
    assertTrue(sysOutStream.toString().contains(
      "Hello container_0_0001_01_000002 in syslog!"));
    assertTrue(sysOutStream.toString().contains(
      "Hello container_0_0001_01_000003 in syslog!"));
    assertTrue(sysOutStream.toString().contains(
      "Hello container_0_0001_01_000003 in stdout!"));
    sysOutStream.reset();

            containerId1.toString() });
    assertTrue(exitCode == 0);
    assertTrue(sysOutStream.toString().contains(
        "Hello container_0_0001_01_000001 in syslog!"));
    assertTrue(sysOutStream.toString().contains("Log Upload Time"));
    assertTrue(!sysOutStream.toString().contains(
      "Logs for container " + containerId1.toString()
    assertTrue(sysOutStream.toString().contains(
      "Logs for container " + containerId0.toString()
          + " are not present in this log-file."));
    sysOutStream.reset();

    exitCode =
        cli.run(new String[] { "-applicationId", appId.toString(),
            "-nodeAddress", nodeId.toString(), "-containerId",
            containerId3.toString() });
    assertTrue(exitCode == 0);
    assertTrue(sysOutStream.toString().contains(
        "Hello container_0_0001_01_000003 in syslog!"));
    assertTrue(sysOutStream.toString().contains(
        "Hello container_0_0001_01_000003 in stdout!"));
    sysOutStream.reset();

    exitCode =
        cli.run(new String[] { "-applicationId", appId.toString(),
            "-nodeAddress", nodeId.toString(), "-containerId",
            containerId3.toString() , "-logFiles", "stdout"});
    assertTrue(exitCode == 0);
    assertTrue(sysOutStream.toString().contains(
        "Hello container_0_0001_01_000003 in stdout!"));
    assertTrue(!sysOutStream.toString().contains(
        "Hello container_0_0001_01_000003 in syslog!"));
    sysOutStream.reset();

    fs.delete(new Path(remoteLogRootDir), true);
    fs.delete(new Path(rootLogDir), true);
  }

  private static void createContainerLogInLocalDir(Path appLogsDir,
      ContainerId containerId, FileSystem fs, List<String> logTypes) throws Exception {
    Path containerLogsDir = new Path(appLogsDir, containerId.toString());
    if (fs.exists(containerLogsDir)) {
      fs.delete(containerLogsDir, true);
    }
    assertTrue(fs.mkdirs(containerLogsDir));
    for (String logType : logTypes) {
      Writer writer =
          new FileWriter(new File(containerLogsDir.toString(), logType));
      writer.write("Hello " + containerId + " in " + logType + "!");
      writer.close();
    }
  }

  private static void uploadContainerLogIntoRemoteDir(UserGroupInformation ugi,
      Configuration configuration, List<String> rootLogDirs, NodeId nodeId,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/logaggregation/AggregatedLogFormat.java
      readAContainerLogsForALogType(valueStream, out, -1);
    }

    public static int readContainerLogsForALogType(
        DataInputStream valueStream, PrintStream out, long logUploadedTime,
        List<String> logType) throws IOException {
      byte[] buf = new byte[65535];

      String fileType = valueStream.readUTF();
      String fileLengthStr = valueStream.readUTF();
      long fileLength = Long.parseLong(fileLengthStr);
      if (logType.contains(fileType)) {
        out.print("LogType:");
        out.println(fileType);
        if (logUploadedTime != -1) {
          out.print("Log Upload Time:");
          out.println(Times.format(logUploadedTime));
        }
        out.print("LogLength:");
        out.println(fileLengthStr);
        out.println("Log Contents:");

        long curRead = 0;
        long pendingRead = fileLength - curRead;
        int toRead = pendingRead > buf.length ? buf.length : (int) pendingRead;
        int len = valueStream.read(buf, 0, toRead);
        while (len != -1 && curRead < fileLength) {
          out.write(buf, 0, len);
          curRead += len;

          pendingRead = fileLength - curRead;
          toRead = pendingRead > buf.length ? buf.length : (int) pendingRead;
          len = valueStream.read(buf, 0, toRead);
        }
        out.println("End of LogType:" + fileType);
        out.println("");
        return 0;
      } else {
        long totalSkipped = 0;
        long currSkipped = 0;
        while (currSkipped != -1 && totalSkipped < fileLength) {
          currSkipped = valueStream.skip(fileLength - totalSkipped);
          totalSkipped += currSkipped;
        }
        return -1;
      }
    }

    public void close() {
      IOUtils.cleanup(LOG, scanner, reader, fsDataIStream);
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/logaggregation/LogCLIHelpers.java
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
  @VisibleForTesting
  public int dumpAContainersLogs(String appId, String containerId,
      String nodeId, String jobOwner) throws IOException {
    return dumpAContainersLogsForALogType(appId, containerId, nodeId, jobOwner,
      null);
  }

  @Private
  @VisibleForTesting
  public int dumpAContainersLogsForALogType(String appId, String containerId,
      String nodeId, String jobOwner, List<String> logType) throws IOException {
    Path remoteRootLogDir = new Path(getConf().get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
          reader =
              new AggregatedLogFormat.LogReader(getConf(),
                thisNodeFile.getPath());
          if (logType == null) {
            if (dumpAContainerLogs(containerId, reader, System.out,
              thisNodeFile.getModificationTime()) > -1) {
              foundContainerLogs = true;
            }
          } else {
            if (dumpAContainerLogsForALogType(containerId, reader, System.out,
              thisNodeFile.getModificationTime(), logType) > -1) {
              foundContainerLogs = true;
            }
          }
        } finally {
          if (reader != null) {
            reader.close();
    return -1;
  }

  @Private
  public int dumpAContainerLogsForALogType(String containerIdStr,
      AggregatedLogFormat.LogReader reader, PrintStream out,
      long logUploadedTime, List<String> logType) throws IOException {
    DataInputStream valueStream;
    LogKey key = new LogKey();
    valueStream = reader.next(key);

    while (valueStream != null && !key.toString().equals(containerIdStr)) {
      key = new LogKey();
      valueStream = reader.next(key);
    }

    if (valueStream == null) {
      return -1;
    }

    boolean foundContainerLogs = false;
    while (true) {
      try {
        int result = LogReader.readContainerLogsForALogType(
            valueStream, out, logUploadedTime, logType);
        if (result == 0) {
          foundContainerLogs = true;
        }
      } catch (EOFException eof) {
        break;
      }
    }

    if (foundContainerLogs) {
      return 0;
    }
    return -1;
  }

  @Private
  public int dumpAllContainersLogs(ApplicationId appId, String appOwner,
      PrintStream out) throws IOException {

