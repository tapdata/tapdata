package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * 各种TapValue写入到目标，数据能被转换成为原始值（依赖WriteRecordFunction）
 * 数据源需要通过dataTypes或者registerCapabilities的方式注册TapValue的转换方式，
 * 这个测试是确保数据源覆盖了所有类型的TapValue转换
 */
@DisplayName("tapValueTest")
@TapGo(tag = "V3", sort = 0, debug = false, ignore = true)
public class EveryTapValueToTargetTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("tapValueTest.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    /**
     * 用例1，FromTapValue的值转换正常
     * 组织11中TapType对应的TapValue，
     * 通过获取该数据源的CodecRegistry，
     * 执行TapCodecsFilterManager#transformFromTapValueMap方法进行值转换，
     * 检查转换后的值， 不应该再包含TapValue
     */
    @DisplayName("tapValueTest.value")
    @TapTestCase(sort = 1)
    @Test
    public void value() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("tapValueTest.value.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {

        });
    }
}