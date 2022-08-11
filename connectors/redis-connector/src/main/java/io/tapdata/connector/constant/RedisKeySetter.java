package io.tapdata.connector.constant;


import io.tapdata.connector.redis.RedisRecordWriter;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 缓存键设置器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/4 下午9:28 Create
 */
public class RedisKeySetter {

  private Map<String, RedisKey> allSet = new HashMap<>();


  /**
   * 获取redis的key
   */
  public String getRedisKey(Map<String, Object> value, TapConnectorContext connectorContext, String tableName) {

    // 兼容数据复制时不存在指定缓存的key。用源表名作为key。
    DataMap nodeConfig = connectorContext.getNodeConfig();
    if (nodeConfig == null) {
      return tableName;
    }
    // 在list with header时根据页面的cachePrefix作为key值
    String valueType = (String) nodeConfig.get("valueType");
    if (RedisRecordWriter.VALUE_TYPE_LIST.equals(valueType)) {
      return (String) nodeConfig.get("cachePrefix");
    }

    RedisKey redisKey = getRedisKey(tableName, connectorContext);


    StringBuilder buf = new StringBuilder();
    // 如果存在缓存前缀，则直接使用缓存前缀
    if (StringUtils.isNotBlank(redisKey.getPrefix())) {
      buf.append(redisKey.getPrefix());
    } else {
      buf.append(tableName);
    }


    String[] keys = redisKey.getVal().split(",");
    for (String key : keys) {
      Object object = value.get(key);
      buf.append("_").append(object2String(object));
    }

    return buf.toString();
  }

  /**
   * 获取redis key的组成字段
   */
  public RedisKey getRedisKey(String tableName, TapConnectorContext connectorContext) {
    // 内存中缓存同一个表名的key值
    RedisKey redisKey = allSet.get(tableName);
    if (null == redisKey) {
      redisKey = init(tableName, connectorContext);
      allSet.put(tableName, redisKey);
    }
    return redisKey;
  }

  /**
   * 初始化redis key的组成
   */
  private RedisKey init(String tableName, TapConnectorContext connectorContext) {

    RedisKey redisKey = new RedisKey();
    DataMap nodeConfig = connectorContext.getNodeConfig();

    List<String> cacheKeys = (List<String>) nodeConfig.get("cacheKeys");

    if (CollectionUtils.isEmpty(cacheKeys)) {
      if (StringUtils.isEmpty(tableName)) {
        throw new RuntimeException("Not found cache key " + tableName);
      }
    }

    redisKey.setVal(String.join(",", cacheKeys));
    redisKey.setPrefix((String) nodeConfig.get("cachePrefix"));

    if (StringUtils.isBlank(redisKey.getVal())) {
      TapLogger.error("Set redis redisKey is '{}'", redisKey.getVal());
      throw new RuntimeException("Redis write failed,rediskey is blank");
    }

    return redisKey;
  }


  private String object2String(Object object) {
    if (object == null) {
      return "null";
    }
    if (object instanceof byte[]) {
      return new String((byte[]) object);
    }
    return object.toString();
  }


}