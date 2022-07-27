//package io.tapdata.connector.constant;
//
//import com.tapdata.entity.Mapping;
//import com.tapdata.entity.RelateDataBaseTable;
//import com.tapdata.entity.RelateDatabaseField;
//import io.tapdata.entity.TargetContext;
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.exception.TargetException;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * 缓存键设置器
// *
// * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
// * @version v1.0 2021/12/4 下午9:28 Create
// */
//public class RedisKeySetter {
//
//  private Map<String, RedisKey> allSet = new HashMap<>();
//
//
//
//  public RedisKey getOrAuto(TapTable tapTable) {
//    RedisKey val = allSet.get(tapTable.getName());
//    if (null == val) {
//      val = init(mapping);
//      allSet.put(tapTable.getName(), val);
//    }
//    return val;
//  }
//
//  private RedisKey init(Mapping mapping) {
//    RedisKey val = new RedisKey(mapping.getRedisKey(), mapping.getRedisKeyPrefix());
//
//    // Check redisKey
//    if (StringUtils.isBlank(mapping.getRedisKey())) {
//      List<String> keys = new ArrayList<>();
//      List<Map<String, String>> joinConditions = mapping.getJoin_condition();
//      if (null == joinConditions || joinConditions.isEmpty()) {  // No primary key
//        RelateDataBaseTable table = null;
//        if (null != targetContext.getTargetConn() && null != targetContext.getTargetConn().getSchema()) {
//          List<RelateDataBaseTable> schemas = targetContext.getTargetConn().getSchema().get("tables");
//          if (null != schemas) {
//            for (RelateDataBaseTable t : schemas) {
//              if (t.getTable_name().equals(mapping.getTo_table())) {
//                table = t;
//                break;
//              }
//            }
//          }
//        }
//        if (null == table)
//          throw new TargetException(true, "Not found target schema '" + mapping.getTo_table() + "'");
//
//        List<RelateDatabaseField> fields = null;
//        for (RelateDatabaseField field : table.getFields()) {
//          if (field.getPrimary_key_position() == 0) {
//            if (null == fields) keys.add(field.getField_name());
//          } else {
//            if (null == fields) fields = new ArrayList<>();
//            fields.add(field);
//          }
//        }
//        if (null != fields) {
//          // Sort by primary key
//          fields.sort((o1, o2) -> {
//            if (o1.getPrimary_key_position() < o2.getPrimary_key_position()) {
//              return 1;
//            } else if (o1.getPrimary_key_position() > o1.getPrimary_key_position()) {
//              return -1;
//            } else {
//              return 0;
//            }
//          });
//          for (RelateDatabaseField f : fields) {
//            keys.add(f.getField_name());
//          }
//        }
//      } else { // Has primary key
//        for (Map<String, String> m : joinConditions) { // {targetField : sourceField}
//          keys.addAll(m.keySet()); // Add all target fields
//        }
//      }
//      val.setVal(String.join(",", keys));
//      if (StringUtils.isBlank(val.getVal())) {
//        throw new RuntimeException("Redis applyop failed,rediskey is blank");
//      }
//      logger.info("Set mapping redisKey is '{}'", val.getVal());
//    }
//
//    // Check redisKeyPrefix
//    if (StringUtils.isBlank(mapping.getRedisKeyPrefix())) {
//      val.setPrefix(mapping.getTo_table());
//      logger.info("Set mapping redisKeyPrefix is '{}'", val.getPrefix());
//    }
//
//    return val;
//  }
//}
