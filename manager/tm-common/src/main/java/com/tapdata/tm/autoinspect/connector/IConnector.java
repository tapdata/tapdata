package com.tapdata.tm.autoinspect.connector;

import com.tapdata.tm.autoinspect.entity.CompareRecord;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 10:34 Create
 */
public interface IConnector extends AutoCloseable {

    ObjectId getConnId();

    String getName();

    IDataCursor<CompareRecord> queryAll(@NonNull String tableName, Object offset);

    CompareRecord queryByKey(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> originalKey, @NonNull LinkedHashSet<String> keyNames);
}
