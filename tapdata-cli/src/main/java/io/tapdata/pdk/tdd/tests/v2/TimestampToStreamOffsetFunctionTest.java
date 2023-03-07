package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("timestamp.test")//TimestampToStreamOffsetFunction基于时间戳返回增量断点
@TapGo(tag = "V2", sort = 80)
public class TimestampToStreamOffsetFunctionTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("timestamp.test.wait"));
        }
    }
    @DisplayName("timestamp.backStreamOffset")//用例1， 通过时间戳能返回增量断点
    @TapTestCase(sort = 1)
    @Test
    /**
     * 方法参数Long time传null的时候能返回当前时间的增量断点， 非空即可。
     * 方法参数Long time传距离当前时间3个小时前的时候能返回那个时间的增量断点， 非空即可。
     * */
    void backStreamOffset() {
        System.out.println(LangUtil.format("timestamp.backStreamOffset.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("backStreamOffset");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
                TimestampToStreamOffsetFunction timestamp = connectorFunctions.getTimestampToStreamOffsetFunction();

                //方法参数Long time传null的时候能返回当前时间的增量断点， 非空即可。
                Object o = timestamp.timestampToStreamOffset(connectorContext, null);
                TapAssert.asserts(() -> {
                    Assertions.assertNotNull(o, LangUtil.format("timestamp.backStreamOffsetWithNull.error"));
                }).acceptAsError(testCase, LangUtil.format("timestamp.backStreamOffsetWithNull.succeed", String.valueOf(o)));

                //方法参数Long time传距离当前时间3个小时前的时候能返回那个时间的增量断点， 非空即可。
                final int timeAgo = 3;
                LocalDateTime localDateTime = LocalDateTime.now().minusHours(timeAgo);
                try {
                    Object o1 = timestamp.timestampToStreamOffset(
                            connectorContext,
                            Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant()).getTime()
                    );
                    TapAssert.asserts(() ->
                            Assertions.assertNotNull(o1, LangUtil.format("timestamp.backStreamOffsetWith.error", timeAgo))
                    ).acceptAsError(testCase, LangUtil.format("timestamp.backStreamOffsetWith.succeed", timeAgo, String.valueOf(o1)));
                } catch (Throwable throwable) {
                    TapAssert.asserts(() -> Assertions.fail(LangUtil.format("timestamp.backStreamOffsetWith.throwable", timeAgo, throwable.getMessage()))).warn(testCase);
                }

            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(TimestampToStreamOffsetFunction.class, LangUtil.format(inNeedFunFormat, "TimestampToStreamOffsetFunction"))
//                support(DropTableFunction.class, LangUtil.format(inNeedFunFormat,"DropTableFunction"))
        );
    }
}
