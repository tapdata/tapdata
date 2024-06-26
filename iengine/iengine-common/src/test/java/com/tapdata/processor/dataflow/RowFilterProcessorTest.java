package com.tapdata.processor.dataflow;

import com.tapdata.entity.MessageEntity;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
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
        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            message = mock(MessageEntity.class);
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
            processContext = mock(HashMap.class);
            ReflectionTestUtils.setField(rowFilterProcessor,"processContext",processContext);
            logger = mock(Logger.class);
            ReflectionTestUtils.setField(rowFilterProcessor,"logger",logger);
        }
        @Test
        @SneakyThrows
        void testSimple(){
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, null)).thenReturn(null);
                when(message.getOp()).thenReturn("u");
                Map<String, Object> before = mock(HashMap.class);
                Map<String, Object> after = mock(HashMap.class);
                when(message.getBefore()).thenReturn(before);
                when(message.getAfter()).thenReturn(after);
                doCallRealMethod().when(rowFilterProcessor).process(message);
                rowFilterProcessor.process(message);
                mb.verify(() -> ScriptUtil.invokeScript(engine, "filter", message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, null),new Times(1));
            }
        }
    }
}
