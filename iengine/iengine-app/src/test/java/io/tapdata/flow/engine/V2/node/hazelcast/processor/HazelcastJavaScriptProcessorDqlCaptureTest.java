package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.tm.commons.dag.process.StandardJsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.aspect.SkipErrorProcessAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTaskManager;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.schema.TapTableMap;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.task.skiperrorevent.SkipErrorEventAspectTask;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reproduces the G13 scenario through the real GraalJS invocation path.
 * The tests cover both direct DQL dispatch and the registered task-session
 * interceptor used by a running task.
 */
class HazelcastJavaScriptProcessorDqlCaptureTest {

    @Test
    void javascriptRuntimeFailureShouldBeCapturedAndFollowingEventShouldContinue() throws Exception {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setTaskRecordId("task-record-1");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setStatus(TaskDto.STATUS_RUNNING);

        StandardJsProcessorNode jsNode = new StandardJsProcessorNode();
        jsNode.setId("js-node-1");
        jsNode.setName("JavaScript");
        jsNode.setScript("function process(record) {\n"
                + "  if (record.scenario === 'SCRIPT_FAIL') {\n"
                + "    throw new Error('DQL_POC_SCRIPT_FAIL');\n"
                + "  }\n"
                + "  return record;\n"
                + "}");
        ProcessorBaseContext context = ProcessorBaseContext.newBuilder()
                .withTaskDto(task)
                .withNode(jsNode)
                .build();

        Invocable engine = ScriptUtil.getScriptEngine(
                JSEngineEnum.GRAALVM_JS.getEngineName(),
                jsNode.getScript(),
                null,
                null,
                null,
                null,
                null,
                (ObsScriptLogger) null,
                true);
        DqlEventReporter reporter = mock(DqlEventReporter.class);
        DqlEventReportResult acknowledgement = new DqlEventReportResult();
        acknowledgement.setEventId("dql-event-1");
        when(reporter.report(anyString(), any())).thenReturn(acknowledgement);

        SkipErrorEventAspectTask dqlTask = configuredDqlTask(task, reporter);
        HazelcastJavaScriptProcessorNode processor = new HazelcastJavaScriptProcessorNode(context) {
            @Override
            protected Invocable getOrInitEngine() {
                return engine;
            }

            @Override
            public AspectInterceptResult executeAspect(Aspect aspect) {
                if (aspect instanceof SkipErrorProcessAspect processAspect) {
                    return dqlTask.skipErrorProcessAspectHandle(processAspect);
                }
                return null;
            }

            @Override
            public boolean needTransformValue() {
                return false;
            }
        };
        ReflectionTestUtils.setField(processor, "standard", true);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal",
                ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<>());

        TapdataEvent failedEvent = event("SCRIPT_FAIL", 1);
        TapdataEvent followingEvent = event("OK", 2);
        List<TapdataEvent> output = assertDoesNotThrow(() -> processor.batchProcess(List.of(
                new HazelcastProcessorBaseNode.BatchEventWrapper(failedEvent),
                new HazelcastProcessorBaseNode.BatchEventWrapper(followingEvent))));

        assertEquals(List.of(followingEvent), output);
        verify(reporter).report(anyString(), any());
    }

    @Test
    void javascriptRuntimeFailureShouldReachTheRegisteredDqlAspect() throws Exception {
        TaskDto task = task();
        task.setSkipErrorEvent(skipConfig());
        StandardJsProcessorNode jsNode = jsNode();
        TapTable processorTable = new TapTable("orders")
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("scenario", "VARCHAR"));
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("js-node-1");
        // In a normal sync task the virtual processor model is indexed by the
        // processor node id, while the failed input event still has the source
        // table name.
        tapTableMap.putNew("js-node-1", processorTable, "processor-orders");
        DataProcessorContext context = DataProcessorContext.newBuilder()
                .withTaskDto(task)
                .withNode(jsNode)
                .withTapTableMap(tapTableMap)
                .build();
        Invocable engine = engine(jsNode.getScript());
        DqlEventReporter reporter = mock(DqlEventReporter.class);
        DqlEventReportResult acknowledgement = new DqlEventReportResult();
        acknowledgement.setEventId("dql-event-registered");
        when(reporter.report(anyString(), any())).thenReturn(acknowledgement);

        ConfigurableApplicationContext applicationContext = mock(ConfigurableApplicationContext.class);
        HttpClientMongoOperator mongoOperator = mock(HttpClientMongoOperator.class);
        SettingService settingService = mock(SettingService.class);
        when(applicationContext.getBean(com.tapdata.mongo.ClientMongoOperator.class)).thenReturn(mongoOperator);
        when(applicationContext.getBean(SettingService.class)).thenReturn(settingService);
        ConfigurableApplicationContext previousContext = BeanUtil.configurableApplicationContext;
        BeanUtil.configurableApplicationContext = applicationContext;
        try {
            AspectTaskManager aspectTaskManager = InstanceFactory.instance(AspectTaskManager.class);
            AspectUtils.executeAspect(new TaskStartAspect().task(task).log(mock(Log.class)));
            List<io.tapdata.aspect.task.AspectTask> registeredTasks = aspectTaskManager.getAspectTasks(task.getId().toString());
            SkipErrorEventAspectTask dqlTask = registeredTasks.stream()
                    .filter(SkipErrorEventAspectTask.class::isInstance)
                    .map(SkipErrorEventAspectTask.class::cast)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("registered aspect tasks: " + registeredTasks.stream().map(item -> item.getClass().getName()).toList()
                            + ", skip error annotation: " + SkipErrorEventAspectTask.class.getAnnotation(io.tapdata.aspect.task.AspectTaskSession.class)
                            + ", class resource: " + SkipErrorEventAspectTask.class.getResource("SkipErrorEventAspectTask.class")));
            ReflectionTestUtils.setField(dqlTask, "dqlRuntimeConfig", DqlRuntimeConfig.defaults());
            ReflectionTestUtils.setField(dqlTask, "dqlEventReporter", reporter);
            ReflectionTestUtils.setField(dqlTask, "nextPrintTimes", Long.MAX_VALUE);
            assertEquals(TaskDto.SkipErrorEvent.ErrorMode.DQL,
                    ((TaskDto.SkipErrorEvent) ReflectionTestUtils.getField(dqlTask, "skipErrorEvent"))
                            .getErrorModeEnum());
            assertEquals(task.getId().toHexString(), ReflectionTestUtils.getField(dqlTask, "taskId"));

            HazelcastJavaScriptProcessorNode processor = new HazelcastJavaScriptProcessorNode(context) {
                @Override
                protected Invocable getOrInitEngine() {
                    return engine;
                }

                @Override
                public boolean needTransformValue() {
                    return false;
                }
            };
            ReflectionTestUtils.setField(processor, "standard", true);
            ReflectionTestUtils.setField(processor, "processContextThreadLocal",
                    ThreadLocal.withInitial(HashMap::new));
            ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<>());
            ReflectionTestUtils.setField(processor, "clientMongoOperator", mongoOperator);

            List<TapdataEvent> output = assertDoesNotThrow(() -> processor.batchProcess(List.of(
                    new HazelcastProcessorBaseNode.BatchEventWrapper(event("SCRIPT_FAIL", 1)),
                    new HazelcastProcessorBaseNode.BatchEventWrapper(event("OK", 2)))));

            assertEquals(1, output.size());
            org.mockito.ArgumentCaptor<DqlEventReport> reportCaptor = forClass(DqlEventReport.class);
            verify(reporter).report(anyString(), reportCaptor.capture());
            assertEquals(DqlRouteDecision.RECORD_DLQ, reportCaptor.getValue().getRouteDecision());
            assertEquals("PROCESSOR", reportCaptor.getValue().getFailedStage());
            assertEquals("30012", reportCaptor.getValue().getErrorCode());
            assertEquals(Map.of("id", 1), reportCaptor.getValue().getEventKey());
            assertEquals(Boolean.FALSE, reportCaptor.getValue().getEventKeyMissing());
            assertEquals("PRIMARY_KEY", reportCaptor.getValue().getRecordIdentityType());
        } finally {
            try {
                AspectUtils.executeAspect(new TaskStopAspect().task(task));
            } catch (Throwable ignored) {
                // Other application aspect tasks need a fully initialized engine
                // context during shutdown; this test only owns the DQL assertion.
            }
            BeanUtil.configurableApplicationContext = previousContext;
        }
    }

    private static TaskDto task() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setTaskRecordId("task-record-1");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setStatus(TaskDto.STATUS_RUNNING);
        return task;
    }

    private static TaskDto.SkipErrorEvent skipConfig() {
        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.DQL);
        // Reproduce a task carrying the legacy skip-limit fields alongside
        // the explicit DQL mode. DQL must not fall back to TASK_ERROR because
        // those fields are zero or stale.
        config.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.SkipByLimit);
        config.setLimit(0L);
        config.setRate(100);
        return config;
    }

    private static StandardJsProcessorNode jsNode() {
        StandardJsProcessorNode jsNode = new StandardJsProcessorNode();
        jsNode.setId("js-node-1");
        jsNode.setName("JavaScript");
        jsNode.setScript("function process(record) {\n"
                + "  if (record.scenario === 'SCRIPT_FAIL') {\n"
                + "    throw new Error('DQL_POC_SCRIPT_FAIL');\n"
                + "  }\n"
                + "  return record;\n"
                + "}");
        return jsNode;
    }

    private static Invocable engine(String script) throws Exception {
        return ScriptUtil.getScriptEngine(
                JSEngineEnum.GRAALVM_JS.getEngineName(), script, null, null, null, null, null,
                (ObsScriptLogger) null, true);
    }

    private static SkipErrorEventAspectTask configuredDqlTask(TaskDto task, DqlEventReporter reporter) {
        SkipErrorEventAspectTask dqlTask = new SkipErrorEventAspectTask();
        dqlTask.setTask(task);
        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.DQL);
        config.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.SkipByLimit);
        config.setLimit(0L);
        config.setRate(100);
        ReflectionTestUtils.setField(dqlTask, "skipErrorEvent", config);
        ReflectionTestUtils.setField(dqlTask, "dqlRuntimeConfig", DqlRuntimeConfig.defaults());
        ReflectionTestUtils.setField(dqlTask, "taskId", task.getId().toHexString());
        ReflectionTestUtils.setField(dqlTask, "dqlEventReporter", reporter);
        ReflectionTestUtils.setField(dqlTask, "logger", mock(io.tapdata.task.skiperrorevent.SplitFileLogger.class));
        ReflectionTestUtils.setField(dqlTask, "nextPrintTimes", Long.MAX_VALUE);
        return dqlTask;
    }

    private static TapdataEvent event(String scenario, int id) {
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", id, "scenario", scenario)));
        return event;
    }
}
