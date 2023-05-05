package io.tapdata.pdk.tdd.tests.v4;

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
 * @author GavinXiao
 * @description BatchReadPauseAndStreamReadTest create by Gavin
 * @create 2023/5/5 19:10
 * 模拟引擎的全量加增量过程， 读取全量过程中sleep停顿， 通过另外一个线程插入， 修改， 删除， 检查数据是否能一致
 **/
@DisplayName("batchPauseAndStream")
@TapGo(tag = "V4", sort = 20000, debug = true, ignore = true)
public class BatchReadPauseAndStreamReadTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("batchPauseAndStream.wait"));
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
     * 用例1，模拟引擎的全量加增量过程，
     * 读取全量过程中sleep停顿，
     * 通过另外一个线程插入，修改，删除，
     * 检查数据是否能一致
     *
     * 全量读取2条数据，
     * 在2条数据前插入一条数据，
     * 由于没有snapshot，
     * 应该能重复读出第二条数据，
     * 此时之后的逻辑是否能正常
     */
    @DisplayName("batchPauseAndStream.batch")
    @TapTestCase(sort = 1)
    @Test
    public void batch() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("batchPauseAndStream.batch.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {

        });
    }
}