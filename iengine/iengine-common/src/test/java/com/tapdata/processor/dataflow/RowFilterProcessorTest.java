package com.tapdata.processor.dataflow;

import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.exception.TapCodeException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class RowFilterProcessorTest {
    private RowFilterProcessor rowFilterProcessor;
    @BeforeEach
    void beforeEach(){
        rowFilterProcessor = mock(RowFilterProcessor.class);
    }
    @Nested
    class processTest{
        private MessageEntity message;
        private ProcessorContext context;
        private Map<String, Object> processContext;
        private Invocable engine;
        private Logger logger;
        private Map<String, Object> before;
        private Map<String, Object> after;
        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            message = new MessageEntity();
            engine = spy(ScriptUtil.getScriptEngine(
                    JSEngineEnum.GRAALVM_JS.getEngineName(),
                    "",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    true));
            ReflectionTestUtils.setField(rowFilterProcessor,"engine",engine);
            context = mock(ProcessorContext.class);
            ReflectionTestUtils.setField(rowFilterProcessor,"context",context);
            processContext = new HashMap<>();
            ReflectionTestUtils.setField(rowFilterProcessor,"processContext",processContext);
            logger = mock(Logger.class);
            ReflectionTestUtils.setField(rowFilterProcessor,"logger",logger);
            before = new HashMap<>();
            after = new HashMap<>();
            before.put("before","test");
            after.put("after","test");
        }
        @Test
        void testUpdateEventSimple(){
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before")).thenReturn(true);
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after")).thenReturn(true);
                message.setOp("u");
                message.setBefore(before);
                message.setAfter(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertEquals(message, actual);
                mb.verify(() -> ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before"),new Times(1));
                mb.verify(() -> ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after"),new Times(1));

            }
        }
        @Test
        void testUpdateEventReturnNull(){
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before")).thenReturn(false);
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after")).thenReturn(false);
                message.setOp("u");
                message.setBefore(before);
                message.setAfter(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertNull(actual);
                mb.verify(() -> ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before"),new Times(1));
                mb.verify(() -> ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after"),new Times(1));

            }
        }
        @Test
        void testUpdateEvent2DeleteEvent() {
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before")).thenReturn(true);
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after")).thenReturn(false);
                message.setOp("u");
                message.setBefore(before);
                message.setAfter(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertEquals("d", actual.getOp());
                assertNull(actual.getAfter());
            }
        }
        @Test
        void testUpdateEvent2InsertEvent() {
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before")).thenReturn(false);
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after")).thenReturn(true);
                message.setOp("u");
                message.setBefore(before);
                message.setAfter(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertEquals("i", actual.getOp());
                assertNull(actual.getBefore());
            }
        }
        @Test
        void testDeleteEventSimple(){
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, null)).thenReturn(true);
                message.setOp("d");
                message.setBefore(before);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertEquals(message, actual);
            }
        }
        @Test
        void testDeleteEventReturnNull(){
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, null)).thenReturn(null);
                message.setOp("d");
                message.setBefore(before);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertNull(actual);
            }
        }
        @Test
        void testUpdateEventWithoutBefore2InsertEvent() {
            ReflectionTestUtils.setField(rowFilterProcessor, "action", RowFilterProcessor.FilterAction.DISCARD);
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before")).thenReturn(false);
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after")).thenReturn(false);
                message.setOp("u");
                message.setBefore(null);
                message.setAfter(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertEquals("i", actual.getOp());
                assertNull(actual.getBefore());
            }
        }
        @Test
        void testUpdateEventWithoutBefore2DeleteEvent() {
            ReflectionTestUtils.setField(rowFilterProcessor, "action", RowFilterProcessor.FilterAction.DISCARD);
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "before")).thenReturn(true);
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, "after")).thenReturn(true);
                message.setOp("u");
                message.setBefore(null);
                message.setAfter(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                MessageEntity actual = rowFilterProcessor.process(message);
                assertEquals("d", actual.getOp());
                assertNull(actual.getAfter());
            }
        }
        @Test
        void test_JAVA_SCRIPT_ERROR() {
            ReflectionTestUtils.setField(rowFilterProcessor, "action", RowFilterProcessor.FilterAction.DISCARD);
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(any(Invocable.class),anyString(),any(MessageEntity.class),eq(null),
                        eq(null),any(Job.class),anyMap(),any(Logger.class),anyString())).thenThrow(Exception.class);
                message.setOp("u");
                message.setBefore(null);
                message.setAfter(after);
                ProcessorContext context = mock(ProcessorContext.class);
                ReflectionTestUtils.setField(rowFilterProcessor, "context", context);
                when(context.getJob()).thenReturn(mock(Job.class));
                doCallRealMethod().when(rowFilterProcessor).process(message);
                rowFilterProcessor.process(message);
            }
        }
    }
}
