package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;


import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Comments implements SchemaStart {
    public final Boolean use = false;
    @Override
    public Boolean use() {
        return use;
    }

    @Override
    public String tableName() {
        return "Comments";
    }

    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }

    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("CommentId", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))
                .add(field("ParentId", "StringMinor"))
                .add(field("RawContent", "StringMinor"))
                .add(field("CreatorId", JAVA_Integer))
                .add(field("IssueCode", JAVA_Integer))
                .add(field("Content", "StringLonger"))
                .add(field("CreatedAt", JAVA_Long))
                .add(field("UpdatedAt", JAVA_Long))   ;
    }
}
