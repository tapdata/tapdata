package com.tapdata.processor;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.TapCodeException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.core.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ScriptUtilTest {

    @Test
    public void testGetScriptEngine3() throws Exception {
        final ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        final ICacheGetter memoryCacheGetter = mock(ICacheGetter.class);
        final Log logger = mock(Log.class);
        final Invocable result = ScriptUtil.getScriptEngine("script", "",null, clientMongoOperator, mock(ScriptConnection.class), mock(ScriptConnection.class),
                memoryCacheGetter, logger);
        Assert.assertNotNull(result);

    }

    @Test
    public void getScriptEngineShouldRejectDangerousJavaTypeAccess() throws Exception {
        final Log logger = new NoopLog();
        String script = "function process(record) {\n" +
                "  Java.type('java.lang.Runtime');\n" +
                "  return record;\n" +
                "}";

        Invocable engine = ScriptUtil.getScriptEngine(
                JSEngineEnum.GRAALVM_JS.getEngineName(),
                script,
                null,
                null,
                null,
                null,
                null,
                logger,
                true);

        assertThrows(Exception.class, () -> engine.invokeFunction("process", new HashMap<>()));
    }

    @Test
    public void getScriptEngineShouldRejectFileJavaTypeAccess() throws Exception {
        final Log logger = new NoopLog();
        String script = "function process(record) {\n" +
                "  Java.type('java.io.File');\n" +
                "  return record;\n" +
                "}";

        Invocable engine = ScriptUtil.getScriptEngine(
                JSEngineEnum.GRAALVM_JS.getEngineName(),
                script,
                null,
                null,
                null,
                null,
                null,
                logger,
                true);

        assertThrows(Exception.class, () -> engine.invokeFunction("process", new HashMap<>()));
    }

    @Test
    public void initBuildInMethodShouldKeepCompatMappingsWithoutDangerousClasses() {
        String buildInMethod = ScriptUtil.initBuildInMethod(null, null, null, false);

        assertTrue(buildInMethod.contains("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");"));
        assertTrue(buildInMethod.contains("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");"));
        assertTrue(buildInMethod.contains("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");"));
        assertTrue(buildInMethod.contains("var sleep = function(ms){"));
        assertTrue(!buildInMethod.contains("java.lang.Runtime"));
        assertTrue(!buildInMethod.contains("java.io.File"));
    }

    @Test
    public void getScriptEngineShouldAllowClassFromExternalClassLoader() throws Exception {
        final Log logger = new NoopLog();
        try (LoggingOutputStream info = new LoggingOutputStream(logger, org.apache.logging.log4j.Level.INFO);
             LoggingOutputStream error = new LoggingOutputStream(logger, org.apache.logging.log4j.Level.ERROR);
             URLClassLoader externalClassLoader = new URLClassLoader(
                     new URL[]{Paths.get("target/test-classes").toUri().toURL()},
                     null)) {

            ScriptEngine engine = ScriptUtil.getScriptEngine(
                    JSEngineEnum.GRAALVM_JS.getEngineName(),
                    info,
                    error,
                    externalClassLoader);

            Object value = engine.eval("Java.type('com.tapdata.processor.ScriptUtilTest$ExternalJarHelper').value()");

            assertEquals("jar-ok", value);
        }
    }

    @Test
    public void getScriptEngineShouldSupportCommonJavaCollectionCallbacks() throws Exception {
        ScriptEngine engine = ScriptUtil.getScriptEngine(JSEngineEnum.GRAALVM_JS.getEngineName());
        try {
            List<Integer> numbers = new ArrayList<>(List.of(3, 1, 2));
            Map<String, Integer> values = new HashMap<>();
            values.put("first", 1);
            engine.put("numbers", numbers);
            engine.put("values", values);

            Object sum = engine.eval("var sum = 0;"
                    + "numbers.forEach(function(value) { sum += value; });"
                    + "numbers.removeIf(function(value) { return value % 2 === 0; });"
                    + "numbers.replaceAll(function(value) { return value * 2; });"
                    + "numbers.sort(function(left, right) { return left - right; });"
                    + "sum;");
            Object mapSum = engine.eval("var mapSum = 0;"
                    + "values.forEach(function(key, value) { mapSum += value; });"
                    + "values.computeIfAbsent('second', function(key) { return 2; });"
                    + "values.compute('first', function(key, value) { return value + 3; });"
                    + "mapSum;");

            assertEquals(6, ((Number) sum).intValue());
            assertEquals(List.of(2, 6), numbers);
            assertEquals(1, ((Number) mapSum).intValue());
            assertEquals(4, values.get("first"));
            assertEquals(2, values.get("second"));
        } finally {
            ((GraalJSScriptEngine) engine).close();
        }
    }

    @Test
    public void getScriptEngineShouldStringifyFilterContainingDateTime() throws Exception {
        ScriptEngine engine = ScriptUtil.getScriptEngine(JSEngineEnum.GRAALVM_JS.getEngineName());
        try {
            engine.put("valDate", new DateTime(Instant.parse("2024-05-08T16:30:45.123Z")));

            Object json = engine.eval("var filter = {"
                    + "employeeNumber: 'E001',"
                    + "salesDateTime: { '$dateTime': valDate },"
                    + "departmentCode: 'D001'"
                    + "}; JSON.stringify(filter);");

            String expected = "{\"employeeNumber\":\"E001\","
                    + "\"salesDateTime\":{\"$dateTime\":\"2024-05-08T16:30:45.123Z\"},"
                    + "\"departmentCode\":\"D001\"}";
            assertEquals(expected, json);
        } finally {
            ((GraalJSScriptEngine) engine).close();
        }
    }

    public static class ExternalJarHelper {
        public static String value() {
            return "jar-ok";
        }
    }

    private static class NoopLog implements Log {
        @Override
        public void debug(String message, Object... params) {
        }

        @Override
        public void info(CharSequence message) {
        }

        @Override
        public void info(String message, Object... params) {
        }

        @Override
        public void trace(CharSequence message) {
        }

        @Override
        public void trace(String message, Object... params) {
        }

        @Override
        public void warn(String message, Object... params) {
        }

        @Override
        public void error(String message, Object... params) {
        }

        @Override
        public void error(String message, Throwable throwable) {
        }

        @Override
        public void fatal(String message, Object... params) {
        }
    }

    @Test
    public void testInvokeScript()  {
        Map<String,Object> input = new HashMap<>();
        assertThrows(TapCodeException.class,()-> ScriptUtil.invokeScript(null,"functionName", mock(MessageEntity.class)
                , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), null));
    }
    @Test
    public void testUrlClassLoader(){
        List<URL> urlList = new ArrayList<>();
        urlList.add(mock(URL.class));
        final ClassLoader[] externalClassLoader = new ClassLoader[1];
        ScriptUtil.urlClassLoader(urlClassLoader -> externalClassLoader[0] = urlClassLoader,urlList);
        Assert.assertNotNull(externalClassLoader[0]);
    }
    @DisplayName("test testUrlClassLoaderException error")
    @Test
    public void testUrlClassLoaderException(){
        List<URL> urlList = new ArrayList<>();
        urlList.add(mock(URL.class));
        final ClassLoader[] externalClassLoader = new ClassLoader[1];
        TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
            ScriptUtil.urlClassLoader(urlClassLoader -> {
                        throw new RuntimeException("appear error");
                    }
                    , urlList);
        });
        assertEquals(ScriptProcessorExCode_30.GET_SCRIPT_ENGINE_ERROR,tapCodeException.getCode());

    }

    @Nested
    public class InvokeScriptWithTagTest{
        private Map<String,Object> input;
        private Invocable engine = mock(Invocable.class);
        private MessageEntity message;
        @BeforeEach
        @SneakyThrows
        void beforeEach(){
            input = new HashMap<>();
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
            message = mock(MessageEntity.class);

        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithBefore(){
            when(message.getBefore()).thenReturn(mock(HashMap.class));
            when(message.getAfter()).thenReturn(mock(HashMap.class));
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), "before");
            verify(message,new Times(2)).getAfter();
            verify(message,new Times(3)).getBefore();
        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithoutBefore(){
            when(message.getAfter()).thenReturn(mock(HashMap.class));
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), "before");
            verify(message,new Times(3)).getAfter();
            verify(message,new Times(2)).getBefore();
        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithAfter(){
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), "after");
            verify(message,new Times(2)).getBefore();
            verify(message,new Times(1)).getAfter();
        }
        @Test
        @SneakyThrows
        public void testInvokeScriptWithNull(){
            ScriptUtil.invokeScript(engine,"process", message
                    , mock(Connections.class), mock(Connections.class), mock(Job.class),input, mock(Logger.class), null);
            verify(message,new Times(2)).getBefore();
            verify(message,new Times(1)).getAfter();
        }
        @SneakyThrows
        @Test
        public void testInvokeScriptWithoutEngine(){
            TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
                ScriptUtil.invokeScript(null, "process", message
                        , mock(Connections.class), mock(Connections.class), mock(Job.class), input, mock(Logger.class), null);
            });
            assertEquals(ScriptProcessorExCode_30.INVOKE_SCRIPT_FAILED_ENGINE_NULL,tapCodeException.getCode());
        }
        @SneakyThrows
        @DisplayName("test Invoke Script function appear INVOKE_SCRIPT_FAILED")
        @Test
        public void test1(){
            when(message.getAfter()).thenReturn(mock(HashMap.class));
            Invocable engine= mock(GraalJSScriptEngine.class);
            when(engine.invokeFunction(anyString(),any())).thenThrow(new ScriptException("script failed"));
            TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
                ScriptUtil.invokeScript(engine, "process", message
                        , mock(Connections.class), mock(Connections.class), mock(Job.class), input, mock(Logger.class), null);
            });
            assertEquals(ScriptProcessorExCode_30.INVOKE_SCRIPT_FAILED,tapCodeException.getCode());

        }
        @Nested
        public class GetPyEngineTest{

            @SneakyThrows
            @Test
            @DisplayName("test get python script with GET_PYTHON_ENGINE_FAILED exception")
            void test1() {
                String initPythonBuildInMethod = ScriptUtil.initPythonBuildInMethod(null, null, ScriptUtilTest.class.getClassLoader(), (classLoader) -> {
                });
                try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = mockStatic(InstanceFactory.class)) {
                    ScriptFactory scriptFactory = mock(ScriptFactory.class);
                    ScriptEngine scriptEngine = mock(ScriptEngine.class);
                    when(scriptEngine.eval(initPythonBuildInMethod)).thenThrow(new ScriptException("eval error"));
                    when(scriptFactory.create(anyString(),any(ScriptOptions.class))).thenReturn(scriptEngine);
                    instanceFactoryMockedStatic.when(()->{InstanceFactory.instance(any(Class.class),anyString());}).thenReturn(scriptFactory);
                    TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
                        Invocable pyEngine = ScriptUtil.getPyEngine(
                                ScriptFactory.TYPE_PYTHON,
                                null,
                                null, //javaScriptFunctions,
                                null,
                                null,
                                null,
                                null,
                                mock(Log.class),
                                ScriptUtilTest.class.getClassLoader());
                    });
                    assertEquals(ScriptProcessorExCode_30.GET_PYTHON_ENGINE_FAILED,tapCodeException.getCode());
                }
            }
            @SneakyThrows
            @Test
            void test2(){
                try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = mockStatic(InstanceFactory.class)) {
                    ScriptFactory scriptFactory = mock(ScriptFactory.class);
                    ScriptEngine scriptEngine = mock(ScriptEngine.class);
                    String scripts = "" + System.lineSeparator() + ScriptUtil.handlePyScript(ScriptUtil.DEFAULT_PY_SCRIPT);
                    when(scriptEngine.eval(scripts)).thenThrow(new ScriptException("eval error"));
                    when(scriptFactory.create(anyString(),any(ScriptOptions.class))).thenReturn(scriptEngine);
                    instanceFactoryMockedStatic.when(()->{InstanceFactory.instance(any(Class.class),anyString());}).thenReturn(scriptFactory);
                    TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
                        Invocable pyEngine = ScriptUtil.getPyEngine(
                                ScriptFactory.TYPE_PYTHON,
                                null,
                                null, //javaScriptFunctions,
                                null,
                                null,
                                null,
                                null,
                                mock(Log.class),
                                ScriptUtilTest.class.getClassLoader());
                    });
                    assertEquals(ScriptProcessorExCode_30.GET_PYTHON_ENGINE_FAILED,tapCodeException.getCode());
                }
            }
        }
    }
}
