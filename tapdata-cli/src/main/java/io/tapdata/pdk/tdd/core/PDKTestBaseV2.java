package io.tapdata.pdk.tdd.core;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static com.tapdata.tm.sdk.util.JacksonUtil.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PDKTestBaseV2 extends PDKTestBase {
    protected LangUtil langUtil = LangUtil.lang(LangUtil.LANG_PATH_V2);

    protected void contrastRecord(TapTable table, Method testMethod, Map<String, Object> basicData, Map<String, Object> targetData) {
        if (Objects.nonNull(basicData) && !basicData.isEmpty()){
            if (Objects.isNull(targetData) || targetData.isEmpty()){
                //TapAssert.error(testMethod,"");
                return;
            }
            StringJoiner basicJoiner = new StringJoiner(",");
            StringJoiner targetJoiner = new StringJoiner(",");
            boolean isBalance = true;
            basicData.forEach((fieldName,fieldValue)->{
                Object value = targetData.get(fieldName);
                if (Objects.isNull(fieldValue)){
                    if (Objects.nonNull(value)){
                        basicJoiner.add(fieldName + ": null" );
                        targetJoiner.add(fieldName + ": " + toJson(value));
                    }
                }else {

                }
            });
        }
    }
}
