package com.tapdata.cache;

import com.tapdata.constant.*;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.exception.DataFlowException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class CacheUtil {

  public final static String CACHE_KEY_SEPERATE = "-";
  public final static Logger logger = LogManager.getLogger(CacheUtil.class);


  public static String cacheDataKey(String cacheName) {
    return "cache" + CACHE_KEY_SEPERATE + cacheName + CACHE_KEY_SEPERATE + "data";
  }

  public static String cacheIndexKey(String cacheName) {
    return "cache" + CACHE_KEY_SEPERATE + cacheName + CACHE_KEY_SEPERATE + "index";
  }

  public static String cacheKey(String pre, Object... cacheKeys) {
    StringBuilder sb = new StringBuilder(pre);
    sb.append(CACHE_KEY_SEPERATE);
    if (cacheKeys != null) {
      for (Object cacheKey : cacheKeys) {
        sb.append(cacheKey).append(CACHE_KEY_SEPERATE);
      }
    }
    return sb.toString();
  }

  public static String cacheKey(Object... cacheKeys) {
    StringBuilder sb = new StringBuilder();
    if (cacheKeys != null) {
      for (Object cacheKey : cacheKeys) {
        sb.append(cacheKey).append(CACHE_KEY_SEPERATE);
      }
    }

    return sb.toString();
  }

  public static Object[] getKeyValues(List<String> keys, Map<String, Object> row) {
    if (CollectionUtils.isEmpty(keys)) {
      return null;
    }

    Object[] keyValues = new Object[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      final String key = keys.get(i);
      if (MapUtil.containsKey(row, key)) {
        keyValues[i] = MapUtil.getValueByKey(row, key);
      } else {
        keyValues[i] = null;
      }
    }
    return keyValues;
  }

  public static Map<String, Object> returnCacheRow(Map<String, Object> result) {

    if (MapUtils.isNotEmpty(result)) {
      Map<String, Object> newMap = new HashMap<>();
      MapUtil.copyToNewMap(result, newMap);
      result = newMap;
    }

    return result;
  }

  public static synchronized void registerCache(Job job, ClientMongoOperator clientMongoOperator, ICacheConfigurator cacheService) {
    List<Stage> stages = job.getStages();
    for (Stage stage : stages) {
      Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(stage.getType());
      if (Stage.StageTypeEnum.MEM_CACHE == stageTypeEnum) {
        Stage inputStage = DataFlowStageUtil.findFirstInputStage(stage, stages);
        if (null == inputStage) {
          throw new RuntimeException("Not found input stage, job id '" + job.getId() + "', stage id '" + stage.getId() + "'");
        }

        String tableName = inputStage.getTableName();
        Query query = new Query(where("id").is(inputStage.getConnectionId()));
        query.fields().exclude("schema").exclude("response_body");
        List<Connections> srcConn = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
        if (CollectionUtils.isEmpty(srcConn)) {
          throw new DataFlowException(String.format("Cannot find stage %s's connection, connection id %s", inputStage.getName(), inputStage.getConnectionId()));
        }
        List<String> pks = null;
        if (StringUtils.isNotBlank(stage.getPrimaryKeys())) {
          String primaryKeys = stage.getPrimaryKeys();
          pks = Arrays.asList(primaryKeys.split(","));
        }

        logger.info("Register cache '" + stage.getCacheName() + "', job id: " + job.getId());
        cacheService.registerCache(new DataFlowCacheConfig(
                stage.getCacheKeys(),
                stage.getCacheName(),
                stage.getCacheType(),
                stage.getMaxRows(),
                stage.getMaxSize(),
                stage.getTtl(),
                stage.getFields(),
                srcConn.get(0),
                null,
                tableName,
                inputStage,
                pks
        ));
      }
    }
    logger.info("Register cache completed: " + job.getId());
  }

  public static synchronized void registerCache(CacheNode cacheNode, TableNode sourceNode, Connections sourceConnection, ClientMongoOperator clientMongoOperator, ICacheConfigurator cacheService) {

    cacheService.registerCache(new DataFlowCacheConfig(
            cacheNode.getCacheKeys(),
            cacheNode.getCacheName(),
            "all",
            cacheNode.getMaxRows(),
            cacheNode.getMaxMemory() == null ? 500L : cacheNode.getMaxMemory(),
            cacheNode.getTtl(),
            new HashSet<>(cacheNode.getFields()),
            sourceConnection,
            sourceNode,
            sourceNode.getTableName(),
            HazelcastUtil.node2CommonStage(sourceNode),
            Collections.emptyList()
    ));
  }

  public static void destroyCache(Job job, ICacheConfigurator cacheService) {
    List<Stage> stages = job.getStages();
    for (Stage stage : stages) {
      Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(stage.getType());
      if (Stage.StageTypeEnum.MEM_CACHE == stageTypeEnum) {
        Stage inputStage = DataFlowStageUtil.findFirstInputStage(stage, stages);
        if (null == inputStage) {
          throw new RuntimeException("Not found input stage, job id '" + job.getId() + "', stage id '" + stage.getId() + "'");
        }
        logger.info("Destroy cache '" + stage.getCacheName() + "', job id: " + job.getId());
        cacheService.destroy(stage.getCacheName());
      }
    }
    logger.info("Destroy cache completed: " + job.getId());
  }

  public static boolean logInfoCacheMetrics(String cacheName,
                                            long currCacheDataSize,
                                            long currRowCount,
                                            long hitCacheCount,
                                            long missCacheCount,
                                            Supplier<Boolean> needToLog) {
    if (needToLog.get()) {
      logger.info("Cache {} data size {}MB, data rows {}, hit cache count {}, miss cache count {}, hit rate {}",
              cacheName,
              byteToMB(currCacheDataSize),
              currRowCount,
              hitCacheCount,
              missCacheCount,
              getHitRate(hitCacheCount, missCacheCount)
      );
//      lastLogTS.put(cacheName, System.currentTimeMillis());
      return true;
    }
    return false;
  }

  public static double getHitRate(long hitCacheCount, long missCacheCount) {
    if (hitCacheCount == 0 && missCacheCount == 0) {
      return 0;
    }
    double hitRate = (double) hitCacheCount / (hitCacheCount + missCacheCount);
    return new BigDecimal(hitRate).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
  }

  public static long byteToMB(long bytes) {
    return bytes / 1024 / 1024;
  }

  public static long byteToKB(long bytes) {
    return bytes / 1024;
  }

  public static DataFlowCacheConfig getCacheConfig(TaskDto taskDto, ClientMongoOperator clientMongoOperator) {
    try {
      DAG dag = taskDto.getDag();
      if (dag == null || dag.getNodes() == null) {
        return null;
      }
      List<Node> nodes = dag.getNodes();
      CacheNode cacheNode = null;
      TableNode tableNode = null;
      for (Node node : nodes) {
        if (node instanceof CacheNode) {
          cacheNode = (CacheNode) node;
        } else if (node.isDataNode()) {
          tableNode = (TableNode) node;
        }
      }
      if (cacheNode == null || tableNode == null) {
        return null;
      }

      String connectionId = tableNode.getConnectionId();
      final Connections sourceConnections = clientMongoOperator.findOne(
              new Query(where("_id").is(connectionId)),
              ConnectorConstant.CONNECTION_COLLECTION,
              Connections.class
      );
      sourceConnections.decodeDatabasePassword();
      sourceConnections.initCustomTimeZone();

      return new DataFlowCacheConfig(
              cacheNode.getCacheKeys(),
              cacheNode.getCacheName(),
              "all",
              cacheNode.getMaxRows(),
              cacheNode.getMaxMemory() == null ? 500L : cacheNode.getMaxMemory(),
              cacheNode.getTtl(),
              new HashSet<>(cacheNode.getFields()),
              sourceConnections,
              tableNode,
              tableNode.getTableName(),
              HazelcastUtil.node2CommonStage(tableNode),
              Collections.emptyList()
      );
    } catch (Exception e) {
      logger.warn("get cache config error", e);
      return null;
    }


  }
}
