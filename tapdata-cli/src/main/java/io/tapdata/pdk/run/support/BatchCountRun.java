package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@DisplayName("batchRead")
@TapGo(sort = 3)
public class BatchCountRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    public void batchCount(){
        consumeQualifiedTapNodeInfo(nodeInfo -> {
//            List<TapEvent> list = new ArrayList<>();
//            PDKTestBase.TestNode prepare = prepare(nodeInfo);
//            RecordEventExecute execute = prepare.recordEventExecute();
//            try {
//                Method testCase = super.getMethod("batchCount");
//                super.connectorOnStart(prepare);
//                execute.testCase(testCase);
//
//
//            } catch (Throwable exception) {
//
//            }
        });
    }
}
