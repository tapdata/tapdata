package io.tapdata.connector.gauss;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Author:Skeet
 * Date: 2023/6/8
 **/
public class GaussWriteRecorder extends WriteRecorder {
    public GaussWriteRecorder(Connection connection, TapTable tapTable, String schema, boolean hasUnique) {
        super(connection, tapTable, schema);
        uniqueConditionIsIndex = uniqueConditionIsIndex && hasUnique;
    }

    public GaussWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {

    }

//    @Override
//    public void addInsertBatch(Map<String, Object> after) throws SQLException {
//        if (EmptyKit.isEmpty(after)) {
//            return;
//        }
//        if (EmptyKit.isNotEmpty(uniqueCondition)) {
//            if (Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
//                if (insertPolicy.equals("ignore-on-exists")) {
//                    conflictIgnoreInsert(after);
//                } else {
//                    conflictUpdateInsert(after);
//                }
//            } else {
//                if (insertPolicy.equals("ignore-on-exists")) {
//                    notExistsInsert(after);
//                } else {
//                    withUpdateInsert(after);
//                }
//            }
//        } else {
//            justInsert(after);
//        }
//        preparedStatement.addBatch();
//    }
}
