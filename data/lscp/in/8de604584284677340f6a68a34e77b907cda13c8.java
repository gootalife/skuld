hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/DecommissionType.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/DecommissionType.java
package org.apache.hadoop.yarn.api.records;

public enum DecommissionType {
  NORMAL,

  GRACEFUL,

  FORCEFUL
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/NodeState.java
  LOST, 
  
  REBOOTED,

  DECOMMISSIONING;
  
  public boolean isUnusable() {
    return (this == UNHEALTHY || this == DECOMMISSIONED || this == LOST);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ResourceManagerAdministrationProtocol.java
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
  @Idempotent
  public UpdateNodeLabelsResponse updateNodeLabels(
      UpdateNodeLabelsRequest request) throws YarnException, IOException;

  @Public
  @Evolving
  @Idempotent
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException;
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/CheckForDecommissioningNodesRequest.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/CheckForDecommissioningNodesRequest.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

@Private
@Unstable
public abstract class CheckForDecommissioningNodesRequest {
  @Private
  @Unstable
  public static CheckForDecommissioningNodesRequest newInstance() {
    CheckForDecommissioningNodesRequest request = Records
        .newRecord(CheckForDecommissioningNodesRequest.class);
    return request;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/CheckForDecommissioningNodesResponse.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/CheckForDecommissioningNodesResponse.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

@Private
@Unstable
public abstract class CheckForDecommissioningNodesResponse {
  @Private
  @Unstable
  public static CheckForDecommissioningNodesResponse newInstance(
      Set<NodeId> decommissioningNodes) {
    CheckForDecommissioningNodesResponse response = Records
        .newRecord(CheckForDecommissioningNodesResponse.class);
    response.setDecommissioningNodes(decommissioningNodes);
    return response;
  }

  public abstract void setDecommissioningNodes(Set<NodeId> decommissioningNodes);

  public abstract Set<NodeId> getDecommissioningNodes();
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/RefreshNodesRequest.java
package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.util.Records;

@Private
@Unstable
public abstract class RefreshNodesRequest {
  @Private
  @Stable
  public static RefreshNodesRequest newInstance() {
    RefreshNodesRequest request = Records.newRecord(RefreshNodesRequest.class);
    return request;
  }

  @Private
  @Unstable
  public static RefreshNodesRequest newInstance(
      DecommissionType decommissionType) {
    RefreshNodesRequest request = Records.newRecord(RefreshNodesRequest.class);
    request.setDecommissionType(decommissionType);
    return request;
  }

  public abstract void setDecommissionType(DecommissionType decommissionType);

  public abstract DecommissionType getDecommissionType();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/RMAdminCLI.java
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
      "No cluster node-labels are specified";
  private static final String NO_MAPPING_ERR_MSG =
      "No node-to-labels mappings are specified";
  private static final String INVALID_TIMEOUT_ERR_MSG =
      "Invalid timeout specified : ";

  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
              "Reload the queues' acls, states and scheduler specific " +
                  "properties. \n\t\tResourceManager will reload the " +
                  "mapred-queues configuration file."))
          .put("-refreshNodes", new UsageInfo("[-g [timeout in seconds]]",
              "Refresh the hosts information at the ResourceManager. Here "
              + "[-g [timeout in seconds] is optional, if we specify the "
              + "timeout then ResourceManager will wait for timeout before "
              + "marking the NodeManager as decommissioned."))
          .put("-refreshSuperUserGroupsConfiguration", new UsageInfo("",
              "Refresh superuser proxy groups mappings"))
          .put("-refreshUserToGroupsMappings", new UsageInfo("",
    summary.append("The full syntax is: \n\n" +
    "yarn rmadmin" +
      " [-refreshQueues]" +
      " [-refreshNodes [-g [timeout in seconds]]]" +
      " [-refreshSuperUserGroupsConfiguration]" +
      " [-refreshUserToGroupsMappings]" +
      " [-refreshAdminAcls]" +
  private int refreshNodes() throws IOException, YarnException {
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest request = RefreshNodesRequest
        .newInstance(DecommissionType.NORMAL);
    adminProtocol.refreshNodes(request);
    return 0;
  }

  private int refreshNodes(long timeout) throws IOException, YarnException {
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    RefreshNodesRequest gracefulRequest = RefreshNodesRequest
        .newInstance(DecommissionType.GRACEFUL);
    adminProtocol.refreshNodes(gracefulRequest);
    CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest = recordFactory
        .newRecordInstance(CheckForDecommissioningNodesRequest.class);
    long waitingTime;
    boolean nodesDecommissioning = true;
    for (waitingTime = 0; waitingTime < timeout || timeout == -1; waitingTime++) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      CheckForDecommissioningNodesResponse checkForDecommissioningNodes = adminProtocol
          .checkForDecommissioningNodes(checkForDecommissioningNodesRequest);
      Set<NodeId> decommissioningNodes = checkForDecommissioningNodes
          .getDecommissioningNodes();
      if (decommissioningNodes.isEmpty()) {
        nodesDecommissioning = false;
        break;
      } else {
        StringBuilder nodes = new StringBuilder();
        for (NodeId nodeId : decommissioningNodes) {
          nodes.append(nodeId).append(",");
        }
        nodes.deleteCharAt(nodes.length() - 1);
        System.out.println("Nodes '" + nodes + "' are still decommissioning.");
      }
    }
    if (nodesDecommissioning) {
      System.out.println("Graceful decommissioning not completed in " + timeout
          + " seconds, issueing forceful decommissioning command.");
      RefreshNodesRequest forcefulRequest = RefreshNodesRequest
          .newInstance(DecommissionType.FORCEFUL);
      adminProtocol.refreshNodes(forcefulRequest);
    } else {
      System.out.println("Graceful decommissioning completed in " + waitingTime
          + " seconds.");
    }
    return 0;
  }

  private int refreshUserToGroupsMappings() throws IOException,
      YarnException {
    if ("-refreshAdminAcls".equals(cmd) || "-refreshQueues".equals(cmd) ||
        "-refreshServiceAcl".equals(cmd) ||
        "-refreshUserToGroupsMappings".equals(cmd) ||
        "-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      if (args.length != 1) {
      if ("-refreshQueues".equals(cmd)) {
        exitCode = refreshQueues();
      } else if ("-refreshNodes".equals(cmd)) {
        if (args.length == 1) {
          exitCode = refreshNodes();
        } else if (args.length == 3) {
          if ("-g".equals(args[1])) {
            long timeout = validateTimeout(args[2]);
            exitCode = refreshNodes(timeout);
          } else {
            printUsage(cmd, isHAEnabled);
            return -1;
          }
        } else {
          printUsage(cmd, isHAEnabled);
          return -1;
        }
      } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
        exitCode = refreshUserToGroupsMappings();
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
    } catch (RemoteException e) {
      exitCode = -1;
      try {
        String[] content;
    return exitCode;
  }

  private long validateTimeout(String strTimeout) {
    long timeout;
    try {
      timeout = Long.parseLong(strTimeout);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(INVALID_TIMEOUT_ERR_MSG + strTimeout);
    }
    if (timeout < -1) {
      throw new IllegalArgumentException(INVALID_TIMEOUT_ERR_MSG + timeout);
    }
    return timeout;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/TestResourceManagerAdministrationProtocolPBClientImpl.java
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
  @Test
  public void testRefreshNodes() throws Exception {
    resourceManager.getClientRMService();
    RefreshNodesRequest request = RefreshNodesRequest
        .newInstance(DecommissionType.NORMAL);
    RefreshNodesResponse response = client.refreshNodes(request);
    assertNotNull(response);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestRMAdminCLI.java

package org.apache.hadoop.yarn.client.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
    verify(admin).refreshNodes(any(RefreshNodesRequest.class));
  }

  @Test
  public void testRefreshNodesWithGracefulTimeout() throws Exception {
    String[] args = { "-refreshNodes", "-g", "1" };
    CheckForDecommissioningNodesResponse response = Records
        .newRecord(CheckForDecommissioningNodesResponse.class);
    HashSet<NodeId> decomNodes = new HashSet<NodeId>();
    response.setDecommissioningNodes(decomNodes);
    when(admin.checkForDecommissioningNodes(any(
        CheckForDecommissioningNodesRequest.class))).thenReturn(response);
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshNodes(
        RefreshNodesRequest.newInstance(DecommissionType.GRACEFUL));

    String[] focefulDecomArgs = { "-refreshNodes", "-g", "1" };
    decomNodes = new HashSet<NodeId>();
    response.setDecommissioningNodes(decomNodes);
    decomNodes.add(NodeId.newInstance("node1", 100));
    response.setDecommissioningNodes(decomNodes);
    when(admin.checkForDecommissioningNodes(any(
        CheckForDecommissioningNodesRequest.class))).thenReturn(response);
    assertEquals(0, rmAdminCLI.run(focefulDecomArgs));
    verify(admin).refreshNodes(
        RefreshNodesRequest.newInstance(DecommissionType.FORCEFUL));

    String[] invalidArgs = { "-refreshNodes", "-ginvalid", "invalid" };
    assertEquals(-1, rmAdminCLI.run(invalidArgs));

    String[] invalidTimeoutArgs = { "-refreshNodes", "-g", "invalid" };
    assertEquals(-1, rmAdminCLI.run(invalidTimeoutArgs));

    String[] negativeTimeoutArgs = { "-refreshNodes", "-g", "-1000" };
    assertEquals(-1, rmAdminCLI.run(negativeTimeoutArgs));
  }

  @Test(timeout=500)
  public void testGetGroups() throws Exception {
    when(admin.getGroupsForUser(eq("admin"))).thenReturn(
      assertTrue(dataOut
          .toString()
          .contains(
              "yarn rmadmin [-refreshQueues] [-refreshNodes [-g [timeout in seconds]]] [-refreshSuper" +
              "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " +
              "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup" +
              " [username]] [[-addToClusterNodeLabels [label1,label2,label3]]" +
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshNodes [-g [timeout in seconds]]: Refresh the hosts information at the " +
              "ResourceManager."));
      assertTrue(dataOut.toString().contains(
          "-refreshUserToGroupsMappings: Refresh user-to-groups mappings"));
      testError(new String[] { "-help", "-refreshQueues" },
          "Usage: yarn rmadmin [-refreshQueues]", dataErr, 0);
      testError(new String[] { "-help", "-refreshNodes" },
          "Usage: yarn rmadmin [-refreshNodes [-g [timeout in seconds]]]", dataErr, 0);
      testError(new String[] { "-help", "-refreshUserToGroupsMappings" },
          "Usage: yarn rmadmin [-refreshUserToGroupsMappings]", dataErr, 0);
      testError(
      assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
      oldOutPrintStream.println(dataOut);
      String expectedHelpMsg = 
          "yarn rmadmin [-refreshQueues] [-refreshNodes [-g [timeout in seconds]]] [-refreshSuper"
              + "UserGroupsConfiguration] [-refreshUserToGroupsMappings] "
              + "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup"
              + " [username]] [[-addToClusterNodeLabels [label1,label2,label3]]"

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/impl/pb/client/ResourceManagerAdministrationProtocolPBClientImpl.java
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
      return null;
    }
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    CheckForDecommissioningNodesRequestProto requestProto =
        ((CheckForDecommissioningNodesRequestPBImpl) checkForDecommissioningNodesRequest)
        .getProto();
    try {
      return new CheckForDecommissioningNodesResponsePBImpl(
          proxy.checkForDecommissioningNodes(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/impl/pb/service/ResourceManagerAdministrationProtocolPBServiceImpl.java
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
      throw new ServiceException(e);
    }
  }

  @Override
  public CheckForDecommissioningNodesResponseProto checkForDecommissioningNodes(
      RpcController controller, CheckForDecommissioningNodesRequestProto proto)
      throws ServiceException {
    CheckForDecommissioningNodesRequest request = new CheckForDecommissioningNodesRequestPBImpl(
        proto);
    try {
      CheckForDecommissioningNodesResponse response = real
          .checkForDecommissioningNodes(request);
      return ((CheckForDecommissioningNodesResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/CheckForDecommissioningNodesRequestPBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/CheckForDecommissioningNodesRequestPBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class CheckForDecommissioningNodesRequestPBImpl extends
    CheckForDecommissioningNodesRequest {

  CheckForDecommissioningNodesRequestProto proto = CheckForDecommissioningNodesRequestProto
      .getDefaultInstance();
  CheckForDecommissioningNodesRequestProto.Builder builder = null;
  boolean viaProto = false;

  public CheckForDecommissioningNodesRequestPBImpl() {
    builder = CheckForDecommissioningNodesRequestProto.newBuilder();
  }

  public CheckForDecommissioningNodesRequestPBImpl(
      CheckForDecommissioningNodesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public CheckForDecommissioningNodesRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/CheckForDecommissioningNodesResponsePBImpl.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/CheckForDecommissioningNodesResponsePBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class CheckForDecommissioningNodesResponsePBImpl extends
    CheckForDecommissioningNodesResponse {

  CheckForDecommissioningNodesResponseProto proto = CheckForDecommissioningNodesResponseProto
      .getDefaultInstance();
  CheckForDecommissioningNodesResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Set<NodeId> decommissioningNodes;

  public CheckForDecommissioningNodesResponsePBImpl() {
    builder = CheckForDecommissioningNodesResponseProto.newBuilder();
  }

  public CheckForDecommissioningNodesResponsePBImpl(
      CheckForDecommissioningNodesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public CheckForDecommissioningNodesResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CheckForDecommissioningNodesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.decommissioningNodes != null) {
      addDecommissioningNodesToProto();
    }
  }

  private void addDecommissioningNodesToProto() {
    maybeInitBuilder();
    builder.clearDecommissioningNodes();
    if (this.decommissioningNodes == null)
      return;
    Set<NodeIdProto> nodeIdProtos = new HashSet<NodeIdProto>();
    for (NodeId nodeId : decommissioningNodes) {
      nodeIdProtos.add(convertToProtoFormat(nodeId));
    }
    builder.addAllDecommissioningNodes(nodeIdProtos);
  }

  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl) nodeId).getProto();
  }

  @Override
  public void setDecommissioningNodes(Set<NodeId> decommissioningNodes) {
    maybeInitBuilder();
    if (decommissioningNodes == null)
      builder.clearDecommissioningNodes();
    this.decommissioningNodes = decommissioningNodes;
  }

  @Override
  public Set<NodeId> getDecommissioningNodes() {
    initNodesDecommissioning();
    return this.decommissioningNodes;
  }

  private void initNodesDecommissioning() {
    if (this.decommissioningNodes != null) {
      return;
    }
    CheckForDecommissioningNodesResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<NodeIdProto> nodeIds = p.getDecommissioningNodesList();
    this.decommissioningNodes = new HashSet<NodeId>();
    for (NodeIdProto nodeIdProto : nodeIds) {
      this.decommissioningNodes.add(convertFromProtoFormat(nodeIdProto));
    }
  }

  private NodeId convertFromProtoFormat(NodeIdProto nodeIdProto) {
    return new NodeIdPBImpl(nodeIdProto);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/RefreshNodesRequestPBImpl.java

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DecommissionTypeProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;

import com.google.protobuf.TextFormat;
  RefreshNodesRequestProto proto = RefreshNodesRequestProto.getDefaultInstance();
  RefreshNodesRequestProto.Builder builder = null;
  boolean viaProto = false;
  private DecommissionType decommissionType;

  public RefreshNodesRequestPBImpl() {
    builder = RefreshNodesRequestProto.newBuilder();
    viaProto = true;
  }
  
  public synchronized RefreshNodesRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.decommissionType != null) {
      builder.setDecommissionType(convertToProtoFormat(this.decommissionType));
    }
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RefreshNodesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public synchronized void setDecommissionType(
      DecommissionType decommissionType) {
    maybeInitBuilder();
    this.decommissionType = decommissionType;
    mergeLocalToBuilder();
  }

  @Override
  public synchronized DecommissionType getDecommissionType() {
    RefreshNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getDecommissionType());
  }

  private DecommissionType convertFromProtoFormat(DecommissionTypeProto p) {
    return DecommissionType.valueOf(p.name());
  }

  private DecommissionTypeProto convertToProtoFormat(DecommissionType t) {
    return DecommissionTypeProto.valueOf(t.name());
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/TestPBImplRecords.java
import org.apache.hadoop.yarn.proto.YarnProtos.YarnClusterMetricsProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
    validatePBImplRecord(UpdateNodeLabelsResponsePBImpl.class,
        UpdateNodeLabelsResponseProto.class);
  }

  @Test
  public void testCheckForDecommissioningNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(CheckForDecommissioningNodesRequestPBImpl.class,
        CheckForDecommissioningNodesRequestProto.class);
  }

  @Test
  public void testCheckForDecommissioningNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(CheckForDecommissioningNodesResponsePBImpl.class,
        CheckForDecommissioningNodesResponseProto.class);
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/AdminService.java
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
      Configuration conf =
          getConfiguration(new Configuration(false),
              YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
      switch (request.getDecommissionType()) {
      case NORMAL:
        rmContext.getNodesListManager().refreshNodes(conf);
        break;
      case GRACEFUL:
        rmContext.getNodesListManager().refreshNodesGracefully(conf);
        break;
      case FORCEFUL:
        rmContext.getNodesListManager().refreshNodesForcefully();
        break;
      }
      RMAuditLogger.logSuccess(user.getShortUserName(), argName,
          "AdminService");
      return recordFactory.newRecordInstance(RefreshNodesResponse.class);
  private void refreshAll() throws ServiceFailedException {
    try {
      refreshQueues(RefreshQueuesRequest.newInstance());
      refreshNodes(RefreshNodesRequest.newInstance(DecommissionType.NORMAL));
      refreshSuperUserGroupsConfiguration(
          RefreshSuperUserGroupsConfigurationRequest.newInstance());
      refreshUserToGroupsMappings(
        "AdminService", "Exception " + msg);
    return RPCUtil.getRemoteException(exception);
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws IOException, YarnException {
    String argName = "checkForDecommissioningNodes";
    final String msg = "check for decommissioning nodes.";
    UserGroupInformation user = checkAcls("checkForDecommissioningNodes");

    checkRMStatus(user.getShortUserName(), argName, msg);

    Set<NodeId> decommissioningNodes = rmContext.getNodesListManager()
        .checkForDecommissioningNodes();
    RMAuditLogger.logSuccess(user.getShortUserName(), argName, "AdminService");
    CheckForDecommissioningNodesResponse response = recordFactory
        .newRecordInstance(CheckForDecommissioningNodesResponse.class);
    response.setDecommissioningNodes(decommissioningNodes);
    return response;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/NodesListManager.java
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;

  public void refreshNodes(Configuration yarnConf) throws IOException,
      YarnException {
    refreshHostsReader(yarnConf);

    for (NodeId nodeId: rmContext.getRMNodes().keySet()) {
      if (!isValidNode(nodeId.getHost())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
      }
    }
  }

  private void refreshHostsReader(Configuration yarnConf) throws IOException,
      YarnException {
    synchronized (hostsReader) {
      if (null == yarnConf) {
        yarnConf = new YarnConfiguration();
                  .getConfigurationInputStream(this.conf, excludesFile));
      printConfiguredHosts();
    }
  }

  private void setDecomissionedNMsMetrics() {
                    .getConfigurationInputStream(this.conf, excludesFile));
    return hostsReader;
  }

  public void refreshNodesGracefully(Configuration conf) throws IOException,
      YarnException {
    refreshHostsReader(conf);
    for (Entry<NodeId, RMNode> entry:rmContext.getRMNodes().entrySet()) {
      NodeId nodeId = entry.getKey();
      if (!isValidNode(nodeId.getHost())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION_WITH_TIMEOUT));
      } else {
        if (entry.getValue().getState() == NodeState.DECOMMISSIONING
            || entry.getValue().getState() == NodeState.DECOMMISSIONED) {
          this.rmContext.getDispatcher().getEventHandler()
              .handle(new RMNodeEvent(nodeId, RMNodeEventType.RECOMMISSION));
        }
      }
    }
  }

  public Set<NodeId> checkForDecommissioningNodes() {
    Set<NodeId> decommissioningNodes = new HashSet<NodeId>();
    for (Entry<NodeId, RMNode> entry : rmContext.getRMNodes().entrySet()) {
      if (entry.getValue().getState() == NodeState.DECOMMISSIONING) {
        decommissioningNodes.add(entry.getKey());
      }
    }
    return decommissioningNodes;
  }

  public void refreshNodesForcefully() {
    for (Entry<NodeId, RMNode> entry : rmContext.getRMNodes().entrySet()) {
      if (entry.getValue().getState() == NodeState.DECOMMISSIONING) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(entry.getKey(), RMNodeEventType.DECOMMISSION));
      }
    }
  }
}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/rmnode/RMNodeEventType.java
  
  DECOMMISSION,
  DECOMMISSION_WITH_TIMEOUT,
  RECOMMISSION,
  
  RESOURCE_UPDATE,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMAdminService.java
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
    rm.start();

    try {
      rm.adminService.refreshNodes(RefreshNodesRequest
          .newInstance(DecommissionType.NORMAL));
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
        + "/excludeHosts");
    uploadConfiguration(yarnConf, YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    rm.adminService.refreshNodes(RefreshNodesRequest
        .newInstance(DecommissionType.NORMAL));
    Set<String> excludeHosts =
        rm.getNodesListManager().getHostsReader().getExcludedHosts();
    Assert.assertTrue(excludeHosts.size() == 1);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestNodesPage.java
public class TestNodesPage {
  
  final int numberOfRacks = 2;
  final int numberOfNodesPerRack = 7;
  final int numberOfLostNodesPerRack = numberOfNodesPerRack

