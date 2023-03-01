package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
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
 *增量验证（依赖StreamReadFunction和TimestampToStreamOffsetFunction）
 * 启动增量可能会比较慢， 超时时间可以设置为20分钟
 * 使用TimestampToStreamOffsetFunction获得当前时间的offset对象， 新建一张表进行增量测试
 * 测试失败按错误上报
 */
@DisplayName("checkStreamReadTest")
@TapGo(tag = "V3", sort = 0, debug = true)
public class CheckStreamReadTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("checkStreamRead.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(StreamReadFunction.class, LangUtil.format(inNeedFunFormat, "StreamReadFunction"))
        );
    }

    /**
     * 用例1，增量启动成功后写入数据
     *   开启增量启动成功之后
     *   StreamReadConsumer#streamReadStarted方法需要被数据源调用;
     *   以下测试依赖WriteRecordFunction
     *   
     *   利用WriteRecordFunction写入3条数据， 能在5秒内通过接收StreamReadConsumer抛出的InsertRecord数据，
     *   来验证数据是按顺序接收到的；
     *
     *   修改其中一条数据的多个字段， 能在5秒内通过接收StreamReadConsumer抛出来的UpdateRecord数据，
     *   来验证UpdateRecord数据的after是包含修改内容的（输出打印， after是全字段还是修改字段， 这个信息是大家会关注的）， before至少包含主键KV或者在after中能找到；
     *
     *   删除其中一条数据， 能在5秒内通过接收StreamReadConsumer抛出来的DeleteRecord数据，
     *   来验证DeleteRecord数据的before是至少包含主键信息的
     */
    @DisplayName("checkStreamRead.batch")
    @TapTestCase(sort = 1)
    @Test
    public void check() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkStreamRead.batch.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {

        });
    }
}