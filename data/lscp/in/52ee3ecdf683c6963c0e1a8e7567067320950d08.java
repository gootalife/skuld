hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoUnderConstruction.java
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        r.getExpectedStorageLocation().removeBlock(this);
        NameNode.blockStateChangeLog.debug("BLOCK* Removing stale replica "
            + "from location: {}", r.getExpectedStorageLocation());
      }
    }
      primary.getExpectedStorageLocation().
          getDatanodeDescriptor().addBlockToBeRecovered(this);
      primary.setChosenAsPrimary(true);
      NameNode.blockStateChangeLog.debug(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
      datanodes.append(node).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.debug("BLOCK* addToInvalidates: {} {}", storedBlock,
          datanodes.toString());
    }
  }
      blockLog.debug("BLOCK* findAndMarkBlockAsCorrupt: {} not found", blk);
      return;
    }

      DatanodeDescriptor node) throws IOException {

    if (b.stored.isDeleted()) {
      blockLog.debug("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
          " corrupt as it does not belong to any file", b);
      addToInvalidates(b.corrupted, node);
      return;
  private boolean invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn
      ) throws IOException {
    blockLog.debug("BLOCK* invalidateBlock: {} on {}", b, dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate " + b
    NumberReplicas nr = countNodes(b.stored);
    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.debug("BLOCK* invalidateBlocks: postponing " +
          "invalidation of {} on {} because {} replica(s) are located on " +
          "nodes with potentially out-of-date block reports", b, dn,
          nr.replicasOnStaleNodes());
          b, dn);
      return true;
    } else {
      blockLog.debug("BLOCK* invalidateBlocks: {} on {} is the only copy and" +
          " was not deleted", b, dn);
      return false;
    }
              if ( (pendingReplications.getNumReplicas(block) > 0) ||
                   (blockHasEnoughRacks(block, requiredReplication)) ) {
                neededReplications.remove(block, priority); // remove from neededReplications
                blockLog.debug("BLOCK* Removing {} from neededReplications as" +
                        " it has enough replicas", block);
                continue;
              }
                 (blockHasEnoughRacks(block, requiredReplication)) ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              rw.targets = null;
              blockLog.debug("BLOCK* Removing {} from neededReplications as" +
                      " it has enough replicas", block);
              continue;
            }
            targetList.append(' ');
            targetList.append(targets[k].getDatanodeDescriptor());
          }
          blockLog.debug("BLOCK* ask {} to replicate {} to {}", rw.srcNode,
              rw.block, targetList);
        }
      }
        }
      }
      if (isCorrupt) {
        blockLog.debug("BLOCK* markBlockReplicasAsCorrupt: mark block replica" +
            " {} on {} as corrupt because the dn is not in the new committed " +
            "storage list.", b, storage.getDatanodeDescriptor());
        markBlockAsCorrupt(b, storage, storage.getDatanodeDescriptor());
    }
    if (storedBlock == null || storedBlock.isDeleted()) {
      blockLog.debug("BLOCK* addStoredBlock: {} on {} size {} but it does not" +
          " belong to any file", block, node, block.getNumBytes());


  private void logAddStoredBlock(BlockInfo storedBlock,
      DatanodeDescriptor node) {
    if (!blockLog.isDebugEnabled()) {
      return;
    }

    storedBlock.appendStringTo(sb);
    sb.append(" size " )
      .append(storedBlock.getNumBytes());
    blockLog.debug(sb.toString());
  }
          removedFromBlocksMap = false;
        }
      } catch (IOException e) {
        blockLog.debug("invalidateCorruptReplicas error in deleting bad block"
            + " {} on {}", blk, node, e);
        removedFromBlocksMap = false;
      }
    addToInvalidates(storedBlock, chosen.getDatanodeDescriptor());
    blockLog.debug("BLOCK* chooseExcessReplicates: "
        +"({}, {}) is added to invalidated blocks set", chosen, storedBlock);
  }

      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.debug("BLOCK* addBlock: logged info for {} of {} reported.",
          maxNumBlocksToLog, numBlocksLogged);
    }
    for (Block b : toInvalidate) {
      blockLog.debug("BLOCK* addBlock: block {} on node {} size {} does not " +
          "belong to any file", b, node, b.getNumBytes());
      addToInvalidates(b, node);
    }
    } finally {
      namesystem.writeUnlock();
    }
    blockLog.debug("BLOCK* {}: ask {} to delete {}", getClass().getSimpleName(),
        dn, toInvalidate);
    return toInvalidate.size();
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/CorruptReplicasMap.java
    }
    
    if (!nodes.keySet().contains(dn)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK NameSystem.addToCorruptReplicasMap: {} added as corrupt on "
              + "{} by {} {}", blk.getBlockName(), dn, Server.getRemoteIp(),
          reasonText);
    } else {
      NameNode.blockStateChangeLog.debug(
          "BLOCK NameSystem.addToCorruptReplicasMap: duplicate requested for" +
              " {} to add as corrupt on {} by {} {}", blk.getBlockName(), dn,
              Server.getRemoteIp(), reasonText);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/InvalidateBlocks.java
    if (set.add(block)) {
      numBlocks++;
      if (log) {
        NameNode.blockStateChangeLog.debug("BLOCK* {}: add {} to {}",
            getClass().getSimpleName(), block, datanode);
      }
    }

