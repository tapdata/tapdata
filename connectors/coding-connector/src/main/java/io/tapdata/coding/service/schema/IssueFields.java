package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class IssueFields implements SchemaStart {
    @Override
    public Boolean use() {
        return false;
    }

    @Override
    public String tableName() {
        return "IssueFields";
    }


    public IssueFields(AtomicReference<String> accessToken) {
    }

    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("IssueFieldId", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))
                .add(field("NeedDefault", JAVA_Boolean))
                .add(field("ValueString", "StringMinor"))
                .add(field("IssueType", "StringMinor"))
                .add(field("Required", JAVA_Boolean))
                .add(field("IssueField", JAVA_Map))
                .add(field("CreatedAt", JAVA_Long))
                .add(field("UpdatedAt", JAVA_Long));
    }

    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }
}
