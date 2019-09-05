hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatPBINode.java
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeEntryProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

      }
    }

    void loadINodeSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      INodeSection s = INodeSection.parseDelimitedFrom(in);
      fsn.dir.resetLastInodeId(s.getLastInodeId());
      long numInodes = s.getNumInodes();
      LOG.info("Loading " + numInodes + " INodes.");
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numInodes);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numInodes; ++i) {
        INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
        if (p.getId() == INodeId.ROOT_INODE_ID) {
          loadRootINode(p);
          INode n = loadINode(p);
          dir.addToInodeMap(n);
        }
        counter.increment();
      }
    }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImageFormatProtobuf.java
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
        case INODE: {
          currentStep = new Step(StepType.INODES);
          prog.beginStep(Phase.LOADING_FSIMAGE, currentStep);
          inodeLoader.loadINodeSection(in, prog, currentStep);
        }
          break;
        case INODE_REFERENCE:
          prog.endStep(Phase.LOADING_FSIMAGE, currentStep);
          Step step = new Step(StepType.DELEGATION_TOKENS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadSecretManagerSection(in, prog, step);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        case CACHE_MANAGER: {
          Step step = new Step(StepType.CACHE_POOLS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadCacheManagerSection(in, prog, step);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
      }
    }

    private void loadSecretManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);
      int numKeys = s.getNumKeys(), numTokens = s.getNumTokens();
      ArrayList<SecretManagerSection.DelegationKey> keys = Lists
      for (int i = 0; i < numKeys; ++i)
        keys.add(SecretManagerSection.DelegationKey.parseDelimitedFrom(in));

      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numTokens);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numTokens; ++i) {
        tokens.add(SecretManagerSection.PersistToken.parseDelimitedFrom(in));
        counter.increment();
      }

      fsn.loadSecretManagerState(s, keys, tokens);
    }

    private void loadCacheManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);
      int numPools = s.getNumPools();
      ArrayList<CachePoolInfoProto> pools = Lists
          .newArrayListWithCapacity(numPools);
      ArrayList<CacheDirectiveInfoProto> directives = Lists
          .newArrayListWithCapacity(s.getNumDirectives());
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numPools);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numPools; ++i) {
        pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
        counter.increment();
      }
      for (int i = 0; i < s.getNumDirectives(); ++i)
        directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
      fsn.getCacheManager().loadState(

