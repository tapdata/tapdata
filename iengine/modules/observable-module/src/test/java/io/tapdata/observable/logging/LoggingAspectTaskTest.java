package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import io.tapdata.aspect.CreateTableFuncAspect;
import io.tapdata.common.SettingService;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/15 10:24
 */
public class LoggingAspectTaskTest {

    @Test
    void testHandlerCreateTableFun() {

        LoggingAspectTask aspectTask = new LoggingAspectTask();

        CreateTableFuncAspect aspect = new CreateTableFuncAspect();

        DataProcessorContext dataProcessContext = mock(DataProcessorContext.class);
        Node node = new DatabaseNode();
        node.setId("nodeId");
        node.setName("nodeName");
        when(dataProcessContext.getNode()).thenReturn(node);
        aspect.setDataProcessorContext(dataProcessContext);
        aspect.setCreateTableEvent(new TapCreateTableEvent());
        aspect.getCreateTableEvent().setTable(new TapTable());

        ConcurrentHashMap<String, ObsLogger> map = mock(ConcurrentHashMap.class);
        ObsLogger logger = mock(ObsLogger.class);
        when(map.computeIfAbsent(anyString(), any())).thenReturn(logger);
        ReflectionTestUtils.setField(aspectTask, "nodeLoggerMap", map);

        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class)) {

            beanUtilMock.when(() -> BeanUtil.getBean(any())).thenAnswer(answer -> {
                Class cls = answer.getArgument(1);
                return mock(cls);
            });

            LoggingAspectTask spyAspectTask = spy(aspectTask);

            aspect.state(CreateTableFuncAspect.STATE_END);
            spyAspectTask.handleCreateTableFuc(aspect);
            aspect.setCreateTableOptions(CreateTableOptions.create());
            spyAspectTask.handleCreateTableFuc(aspect);
            aspect.getCreateTableOptions().tableExists(true);
            spyAspectTask.handleCreateTableFuc(aspect);
            aspect.getCreateTableOptions().tableExists(false);
            spyAspectTask.handleCreateTableFuc(aspect);

            verify(logger, times(1)).info(anyString(), any());

        }

    }

}
