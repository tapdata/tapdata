package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Projects implements SchemaStart {
    public final Boolean use = false;

    @Override
    public Boolean use() {
        return use;
    }

    public Projects(AtomicReference<String> accessToken) {
    }

    @Override
    public String tableName() {
        return "Projects";
    }

    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }

    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("Id", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))
                .add(field("CreatedAt", JAVA_Long).isPrimaryKey(true).primaryKeyPos(2))
                .add(field("UpdatedAt", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
                .add(field("Status", JAVA_Integer))
                .add(field("Type", JAVA_Integer))
                .add(field("MaxMember", JAVA_Integer))
                .add(field("Name", ""))
                .add(field("DisplayName", "StringMinor"))
                .add(field("Description", "StringLonger"))
                .add(field("Icon", JAVA_Integer))
                .add(field("TeamOwnerId", JAVA_Integer))
                .add(field("UserOwnerId", JAVA_Integer))
                .add(field("StartDate", JAVA_Long))
                .add(field("EndDate", JAVA_Long))
                .add(field("TeamId", JAVA_Integer))
                .add(field("IsDemo", JAVA_Boolean))
                .add(field("Archived", JAVA_Boolean))
                ;
    }
}
