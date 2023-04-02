package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Boolean;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Integer;

public class IssueTypes implements SchemaStart {
    @Override
    public Boolean use() {
        return false;
    }

    public IssueTypes(AtomicReference<String> accessToken) {
    }

    @Override
    public String tableName() {
        return "IssueTypes";
    }

    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }

    /**
     * {
     * "Id": 1,
     * "Name": "用户故事",
     * "IssueType": "REQUIREMENT",
     * "Description": "",
     * "IsSystem": false
     * }
     */
    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("Id", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))
                .add(field("Name", "StringMinor"))
                .add(field("IssueType", "StringMinor"))
                .add(field("IsSystem", JAVA_Boolean))
                .add(field("Description", "StringLonger"));
    }

}
