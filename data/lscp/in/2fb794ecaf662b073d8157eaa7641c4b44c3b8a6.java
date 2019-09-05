hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ResourceRequestPBImpl.java
    return "{Priority: " + getPriority() + ", Capability: " + getCapability()
        + ", # Containers: " + getNumContainers()
        + ", Location: " + getResourceName()
        + ", Relax Locality: " + getRelaxLocality()
        + ", Node Label Expression: " + getNodeLabelExpression() + "}";
  }

  @Override

