package com.tapdata.tm.autoinspect.connector;

import com.tapdata.tm.autoinspect.entity.CompareEvent;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import org.bson.types.ObjectId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 10:34 Create
 */
public interface IConnector extends AutoCloseable {

    ObjectId getConnId();
    String getName();

    IDataCursor<CompareRecord> queryAll(String tableName, Object offset);

    void increment(Function<List<CompareEvent>, Boolean> compareEventConsumer);

    CompareRecord queryByKey(String tableName, LinkedHashMap<String, Object> keymap);
}
