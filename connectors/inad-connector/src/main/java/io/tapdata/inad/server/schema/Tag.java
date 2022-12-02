package io.tapdata.inad.server.schema;

import io.tapdata.entity.schema.TapTable;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Tag  implements Schema{
    @Override
    public TapTable schema() {
        return table(tableName())
                .add(field("openid", JAVA_String).tapType(tapString().bytes(1100l)))//手机号（多个openid以逗号隔开，最多100个openid）
                .add(field("tag_str", JAVA_String))//标签id_string(通过其他接口获取，或请技术人员提供)(多个以逗号隔开，最多50个)
                .add(field("tag_names", JAVA_String))//标签名称串(通过其他接口获取，或请技术人员提供)(多个以逗号隔开，最多50个)
                .add(field("tag_time", JAVA_Integer))//打标签时间,仅setUserTagWithTime接口支持
                ;
    }

    @Override
    public String tableName() {
        return "Tag";
    }
}
