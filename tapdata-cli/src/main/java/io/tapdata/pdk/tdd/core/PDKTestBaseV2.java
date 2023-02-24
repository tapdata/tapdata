package io.tapdata.pdk.tdd.core;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.tdd.core.base.TestExec;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.core.base.TestStart;
import io.tapdata.pdk.tdd.core.base.TestStop;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static com.tapdata.tm.sdk.util.JacksonUtil.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PDKTestBaseV2 extends PDKTestBase {
    protected static final LangUtil langUtil = LangUtil.lang(LangUtil.LANG_PATH_V2);

    protected void contrastRecord(TapTable table, Method testMethod, Map<String, Object> basicData, Map<String, Object> targetData) {
        if (Objects.nonNull(basicData) && !basicData.isEmpty()) {
            if (Objects.isNull(targetData) || targetData.isEmpty()) {
                //TapAssert.error(testMethod,"");
                return;
            }
            StringJoiner basicJoiner = new StringJoiner(",");
            StringJoiner targetJoiner = new StringJoiner(",");
            boolean isBalance = true;
            basicData.forEach((fieldName, fieldValue) -> {
                Object value = targetData.get(fieldName);
                if (Objects.isNull(fieldValue)) {
                    if (Objects.nonNull(value)) {
                        basicJoiner.add(fieldName + ": null");
                        targetJoiner.add(fieldName + ": " + toJson(value));
                    }
                } else {

                }
            });
        }
    }

    protected void execTest(String testCaseName, TestStart start, TestExec exec, TestStop stop) throws NoSuchMethodException {
        Method testCase = super.getMethod(testCaseName);
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                Optional.ofNullable(start).ifPresent(TestStart::start);
                super.connectorOnStart(prepare);
                Optional.ofNullable(exec).ifPresent(e->e.exec(prepare,testCase));
            } catch (Exception e) {
                TapAssert.error(testCase, this.langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
            } finally {
                Optional.ofNullable(stop).ifPresent(TestStop::stop);
                super.connectorOnStop(prepare);
            }
        });
    }
    protected void execTest(String testCaseName,TestExec exec) throws NoSuchMethodException {
        this.execTest(testCaseName,null,exec,null);
    }
    protected void execTest(String testCaseName,TestExec exec, TestStop stop) throws NoSuchMethodException {
        this.execTest(testCaseName,null,exec,stop);
    }
}




