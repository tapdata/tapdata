package io.tapdata.connector.constant;


import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;


import java.util.*;

/**
 * 缓存键设置器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/4 下午9:28 Create
 */
public class RedisKeySetter {

  private  Map<String, RedisKey> allSet = new HashMap<>();

  private  List arrayList = new ArrayList();


  public  String getRedisKey(Map<String, Object> value, TapTable tapTable, TapConnectorContext connectorContext) {
    RedisKey redisKey = getOrAuto(tapTable,connectorContext);

    StringBuffer buf = new StringBuffer();
    if (StringUtils.isNotBlank(redisKey.getPrefix())) {
      buf.append(redisKey.getPrefix());
    }

    String[] keys = redisKey.getVal().split(",");
    for (String key : keys) {
      Object object = value.get(key);
      buf.append("_").append(object2String(object));
    }

    return buf.toString();
  }


  public RedisKey getOrAuto(TapTable tapTable,TapConnectorContext connectorContext) {
    RedisKey val = allSet.get(tapTable.getName());
    if (null == val) {
      val = init(tapTable);
      allSet.put(tapTable.getName(), val);
    }
    return val;
  }

  private RedisKey init(TapTable tapTable) {

    RedisKey redisKey = new RedisKey();
    List<String> keys = new ArrayList<>();
    // No primary key
    if (CollectionUtils.isEmpty(tapTable.getDefaultPrimaryKeys())) {
      if (null == tapTable.getName()) {
        throw new RuntimeException("Not found target schema " + tapTable.getName());
      }
      List<TapField> fields = new ArrayList<>(10);
      for (TapField field : tapTable.getNameFieldMap().values()) {
        fields.add(field);
      }
      if (null != fields) {
        // Sort by primary key
        fields.sort((o1, o2) -> {
          if (o1.getPos() < o2.getPos()) {
            return 1;
          } else if (o1.getPos() > o1.getPos()) {
            return -1;
          } else {
            return 0;
          }
        });

        for (TapField tapField : fields) {
          keys.add(tapField.getName());
        }
      }

    } else { //  primary key
      for (String primary : tapTable.getDefaultPrimaryKeys()) {
        keys.add(primary);
      }
    }

      redisKey.setVal(String.join(",", keys));
      redisKey.setPrefix(tapTable.getName());

      if (StringUtils.isBlank(redisKey.getVal())) {
        TapLogger.info("Set redis redisKey is '{}'", redisKey.getVal());
        throw new RuntimeException("Redis applyop failed,rediskey is blank");
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


  public boolean tableIsExist(String tableName){

    if(!arrayList.contains(tableName)){
      arrayList.add(tableName);
      return true;
    }
    return false;
  }


}
