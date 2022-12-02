package io.tapdata.inad.server.schema;

import io.tapdata.entity.schema.TapTable;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Message implements Schema{
    @Override
    public TapTable schema() {
        return table(tableName())
                .add(field("post", JAVA_Map))
                .add(field("mian_id", JAVA_String))
                .add(field("appid", JAVA_String))
                .add(field("wx_id", JAVA_String))
                .add(field("remark", JAVA_String))
                ;
    }

    @Override
    public String tableName() {
        return "Message";
    }
}
