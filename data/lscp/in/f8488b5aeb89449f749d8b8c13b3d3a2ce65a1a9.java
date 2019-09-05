hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppAttemptBlock.java
    createAttemptHeadRoomTable(html);
    html._(InfoBlock.class);

    createTablesForAttemptMetrics(html);

    TBODY<TABLE<Hamlet>> tbody =
        html.table("#containers").thead().tr().th(".id", "Container ID")
  protected void createAttemptHeadRoomTable(Block html) {
    
  }

  protected void createTablesForAttemptMetrics(Block html) {

  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppAttemptBlock.java
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import com.google.inject.Inject;
import java.util.List;

import java.util.Collection;
import java.util.Set;
    this.conf = conf;
  }

  private void createResourceRequestsTable(Block html) {
    AppInfo app =
        new AppInfo(rm, rm.getRMContext().getRMApps()
          .get(this.appAttemptId.getApplicationId()), true,
          WebAppUtils.getHttpSchemePrefix(conf));

    List<ResourceRequest> resourceRequests = app.getResourceRequests();
    if (resourceRequests == null || resourceRequests.isEmpty()) {
      return;
    }

    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3("Total Outstanding Resource Requests: "
          + getTotalResource(resourceRequests)).table(
              "#ResourceRequests");

    table.tr().
      th(_TH, "Priority").
      th(_TH, "ResourceName").
      th(_TH, "Capability").
      th(_TH, "NumContainers").
      th(_TH, "RelaxLocality").
      th(_TH, "NodeLabelExpression").
    _();

    boolean odd = false;
    for (ResourceRequest request : resourceRequests) {
      if (request.getNumContainers() == 0) {
        continue;
      }
      table.tr((odd = !odd) ? _ODD : _EVEN)
        .td(String.valueOf(request.getPriority()))
        .td(request.getResourceName())
        .td(String.valueOf(request.getCapability()))
        .td(String.valueOf(request.getRelaxLocality()))
        .td(request.getNodeLabelExpression() == null ? "N/A" : request
            .getNodeLabelExpression())._();
    }
    table._();
    div._();
  }

  private Resource getTotalResource(List<ResourceRequest> requests) {
    Resource totalResource = Resource.newInstance(0, 0);
    if (requests == null) {
      return totalResource;
    }
    for (ResourceRequest request : requests) {
      if (request.getNumContainers() == 0) {
        continue;
      }
      if (request.getResourceName().equals(ResourceRequest.ANY)) {
        Resources.addTo(
          totalResource,
          Resources.multiply(request.getCapability(),
            request.getNumContainers()));
      }
    }
    return totalResource;
  }

  private void createContainerLocalityTable(Block html) {
    }
    return null;
  }

  @Override
  protected void createTablesForAttemptMetrics(Block html) {
    createContainerLocalityTable(html);
    createResourceRequestsTable(html);
  }
}

