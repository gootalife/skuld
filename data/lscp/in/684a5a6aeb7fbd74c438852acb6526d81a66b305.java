hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ResourceManagerAdministrationProtocol.java
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;

@Private
public interface ResourceManagerAdministrationProtocol extends GetUserMappingsProtocol {

  @Private
  @Idempotent
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request) 
  throws StandbyException, YarnException, IOException;

  @Private
  @Idempotent
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
  throws StandbyException, YarnException, IOException;

  @Private
  @Idempotent
  public RefreshSuperUserGroupsConfigurationResponse 
  refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
  throws StandbyException, YarnException, IOException;

  @Private
  @Idempotent
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
  throws StandbyException, YarnException, IOException;

  @Private
  @Idempotent
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request)
  throws YarnException, IOException;

  @Private
  @Idempotent
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request)
  @Private
  @Idempotent
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) 
  throws YarnException, IOException;
   
  @Private
  @Idempotent
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException;
   
  @Private
  @Idempotent
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request) throws YarnException, IOException;
  
  @Private
  @Idempotent
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException;
  
  @Private
  @Idempotent
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)

