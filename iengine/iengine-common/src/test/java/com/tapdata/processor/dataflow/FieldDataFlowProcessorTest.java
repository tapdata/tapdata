package com.tapdata.processor.dataflow;

import com.tapdata.entity.FieldProcess;
import com.tapdata.entity.FieldScript;
import com.tapdata.entity.MessageEntity;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class FieldDataFlowProcessorTest {
    private FieldDataFlowProcessor fieldDataFlowProcessor;
    @BeforeEach
    void beforeEach(){
        fieldDataFlowProcessor = mock(FieldDataFlowProcessor.class);
    }
    @Nested
    class processTest{
        private MessageEntity message;
        private List<FieldProcess> fieldProcesses;
        @BeforeEach
        void beforeEach() {
            message = mock(MessageEntity.class);
            fieldProcesses = new ArrayList<>();
            ReflectionTestUtils.setField(fieldDataFlowProcessor, "fieldProcesses", fieldProcesses);
        }
        @Test
        @SneakyThrows
        void testSimple(){
            when(message.getOp()).thenReturn("u");
            Map<String, Object> before = mock(HashMap.class);
            Map<String, Object> after = mock(HashMap.class);
            when(message.getBefore()).thenReturn(before);
            when(message.getAfter()).thenReturn(after);
            doNothing().when(fieldDataFlowProcessor).fieldScript(any(MessageEntity.class),any(Map.class),anyString());
            doCallRealMethod().when(fieldDataFlowProcessor).process(message);
            fieldDataFlowProcessor.process(message);
            verify(fieldDataFlowProcessor, new Times(1)).fieldScript(message,before,"before");
            verify(fieldDataFlowProcessor, new Times(1)).fieldScript(message,after,"after");
        }
        @Test
        @SneakyThrows
        void testSimpleWithoutBefore(){
            when(message.getOp()).thenReturn("u");
            Map<String, Object> before = mock(HashMap.class);
            Map<String, Object> after = mock(HashMap.class);
            when(message.getBefore()).thenReturn(null);
            when(message.getAfter()).thenReturn(after);
            doNothing().when(fieldDataFlowProcessor).fieldScript(any(MessageEntity.class),any(Map.class),anyString());
            doCallRealMethod().when(fieldDataFlowProcessor).process(message);
            fieldDataFlowProcessor.process(message);
            verify(fieldDataFlowProcessor, new Times(0)).fieldScript(message,before,"before");
            verify(fieldDataFlowProcessor, new Times(1)).fieldScript(message,after,"after");
        }
        @Test
        @SneakyThrows
        void testSimpleWithoutAfter(){
            when(message.getOp()).thenReturn("u");
            Map<String, Object> before = mock(HashMap.class);
            Map<String, Object> after = mock(HashMap.class);
            when(message.getBefore()).thenReturn(before);
            when(message.getAfter()).thenReturn(null);
            doNothing().when(fieldDataFlowProcessor).fieldScript(any(MessageEntity.class),any(Map.class),anyString());
            doCallRealMethod().when(fieldDataFlowProcessor).process(message);
            fieldDataFlowProcessor.process(message);
            verify(fieldDataFlowProcessor, new Times(1)).fieldScript(message,before,"before");
            verify(fieldDataFlowProcessor, new Times(0)).fieldScript(message,after,"after");
        }
    }
    @Nested
    class fieldScriptTest{
        private MessageEntity message;
        private Map<String, Object> record;
        private String tag;
        private ProcessorContext context;
        private Map<String, Object> processContext;
        private Invocable engine;
        private Logger logger;
        @BeforeEach
        @SneakyThrows
        void beforeEach(){
            message = mock(MessageEntity.class);
            record = mock(HashMap.class);
            tag = null;
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
            context = mock(ProcessorContext.class);
            ReflectionTestUtils.setField(fieldDataFlowProcessor,"context",context);
            processContext = mock(HashMap.class);
            ReflectionTestUtils.setField(fieldDataFlowProcessor,"processContext",processContext);
            logger = mock(Logger.class);
            ReflectionTestUtils.setField(fieldDataFlowProcessor,"logger",logger);
        }
        @Test
        void testSimple(){
            try (MockedStatic<ScriptUtil> mb = Mockito
                    .mockStatic(ScriptUtil.class)) {
                mb.when(()->ScriptUtil.invokeScript(engine,ScriptUtil.FUNCTION_NAME, message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, tag)).thenReturn(null);
                List<FieldScript> fieldScripts = new ArrayList<>();
                FieldScript fieldScript = new FieldScript();
                fieldScript.setField("doc_id");
                fieldScript.setScript("function process(record) {return record;}");
                fieldScripts.add(fieldScript);
                ReflectionTestUtils.setField(fieldDataFlowProcessor, "fieldScripts", fieldScripts);

                when(fieldDataFlowProcessor.getOrInitEngine(eq("doc_id"), anyString())).thenReturn(engine);
                doCallRealMethod().when(fieldDataFlowProcessor).fieldScript(message,record,tag);
                fieldDataFlowProcessor.fieldScript(message,record,tag);
                mb.verify(() -> ScriptUtil.invokeScript(engine,ScriptUtil.FUNCTION_NAME, message, context.getSourceConn(),
                        context.getTargetConn(), context.getJob(), processContext, logger, tag),new Times(1));
            }
        }
    }
}
