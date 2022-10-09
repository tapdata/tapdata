package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class IssueTypes implements SchemaStart {
    @Override
    public Boolean use() {
        return true;
    }

    @Override
    public String tableName() {
        return "IssueTypes";
    }

    /**
     * {
     *       "Id": 1,
     *       "Name": "用户故事",
     *       "IssueType": "REQUIREMENT",
     *       "Description": "",
     *       "IsSystem": false
     *     }
     * */
    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("Id", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))
                .add(field("Name", "StringMinor"))
                .add(field("IssueType", "StringMinor"))
                .add(field("IsSystem", JAVA_Boolean))
                .add(field("Description", "StringLonger"));
    }

    @Override
    public TapTable csv(TapConnectionContext connectionContext) {
        return null;
    }

    @Override
    public Map<String, Object> autoSchema(Map<String, Object> eventData) {
        return null;
    }
}
