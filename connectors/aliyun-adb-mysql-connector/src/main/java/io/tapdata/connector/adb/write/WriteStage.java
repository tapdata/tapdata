package io.tapdata.connector.adb.write;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * @author GavinXiao
 * @description WriteStage create by Gavin
 * @create 2023/6/5 16:51
 **/
public interface WriteStage {

    //相同时需要移除主键
    public default void removePrimaryKeys(TapTable table, TapUpdateRecordEvent event){
        Collection<String> primaryKeys = table.primaryKeys(false);
        filterMap(event.getAfter(), primaryKeys);
        filterMap(event.getBefore(), primaryKeys);
    }

    //不相同则需要拆成删除和插入或者报错提示
    public int splitToInsertAndDeleteFromUpdate(TapConnectorContext context, TapUpdateRecordEvent event, TapTable table) throws Throwable;


    //判断before和after是否有主键的变化
    //主键改变后，返回true
    public default boolean hasEqualsValueOfPrimaryKey(TapConnectorContext tapConnectorContext, TapUpdateRecordEvent tapRecordEvent, TapTable tapTable){
        if (Objects.isNull(tapTable)){
            throw new CoreException("TapTable can not be empty, update event will be cancel");
        }
        Map<String, Object> before = tapRecordEvent.getBefore();
        if (null == before || before.isEmpty()){
            return true;
        }
        Map<String, Object> after = tapRecordEvent.getAfter();
        if (null == after || after.isEmpty()){
            return true;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys(false);
        if (null != primaryKeys && !primaryKeys.isEmpty()){
            for (String key : primaryKeys) {
                if (Objects.isNull(key) || "".equals(key.trim())) continue;
                Object valueAfter = after.get(key);
                Object valueBefore = before.get(key);
                if ((null != valueAfter && !valueAfter.equals(valueBefore))
                        || (null != valueBefore && !valueBefore.equals(valueAfter))){
                    return false;
                }
            }
        }
        return true;
    }

    //移除map中指定的key
    public default Map<String, Object> filterMap(Map<String, Object> map, Collection<String> filterKey){
        if (null == filterKey || filterKey.isEmpty() || null == map || map.isEmpty()) return map;
        for (String key : filterKey) {
            map.remove(key);
        }
        return map;
    }
}
