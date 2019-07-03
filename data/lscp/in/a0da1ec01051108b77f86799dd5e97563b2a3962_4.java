hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ApplicationReportPBImpl.java
 import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
 import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
 import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
 import org.apache.hadoop.yarn.api.records.Priority;
 import org.apache.hadoop.yarn.api.records.Token;
 import org.apache.hadoop.yarn.api.records.YarnApplicationState;
 import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
 import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
 import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
 import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
 import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
 import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
 
 import com.google.protobuf.TextFormat;
   private Token clientToAMToken = null;
   private Token amRmToken = null;
   private Set<String> applicationTags = null;
   private Priority priority = null;
 
   public ApplicationReportPBImpl() {
     builder = ApplicationReportProto.newBuilder();
       builder.clearApplicationTags();
       builder.addAllApplicationTags(this.applicationTags);
     }
     if (this.priority != null
         && !((PriorityPBImpl) this.priority).getProto().equals(
             builder.getPriority())) {
       builder.setPriority(convertToProtoFormat(this.priority));
     }
   }
 
   private void mergeLocalToProto() {
     return ((TokenPBImpl)t).getProto();
   }
 
   private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
     return new PriorityPBImpl(p);
   }
 
   private PriorityProto convertToProtoFormat(Priority t) {
     return ((PriorityPBImpl)t).getProto();
   }
 
   @Override
   public LogAggregationStatus getLogAggregationStatus() {
     ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
     maybeInitBuilder();
     builder.setUnmanagedApplication(unmanagedApplication);
   }
 
   @Override
   public Priority getPriority() {
     ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
     if (this.priority != null) {
       return this.priority;
     }
     if (!p.hasPriority()) {
       return null;
     }
     this.priority = convertFromProtoFormat(p.getPriority());
     return this.priority;
   }
 
   @Override
   public void setPriority(Priority priority) {
     maybeInitBuilder();
     if (priority == null)
       builder.clearPriority();
     this.priority = priority;
   }
 }

