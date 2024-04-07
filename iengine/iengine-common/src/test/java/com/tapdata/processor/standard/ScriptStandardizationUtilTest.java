package com.tapdata.processor.standard;


import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.TapCodeException;
import io.tapdata.js.connector.base.JsUtil;
import io.tapdata.utils.AppType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScriptStandardizationUtilTest {
    List<JavaScriptFunctions> javaScriptFunctions;
    JavaScriptFunctions javaScriptFunction;
    ClientMongoOperator clientMongoOperator;
    ICacheGetter memoryCacheGetter;
    Log logger;

    @BeforeEach
    void init() {
        javaScriptFunctions = new ArrayList<>();
        javaScriptFunction = mock(JavaScriptFunctions.class);
        javaScriptFunctions.add(javaScriptFunction);

        clientMongoOperator = mock(ClientMongoOperator.class);
        memoryCacheGetter = mock(ICacheGetter.class);
        logger = mock(Log.class);
    }

    @Nested
    class GetScriptStandardizationEngineBeforeTest {
        ClassLoader[] externalClassLoader;
        URLClassLoader loader;
        @BeforeEach
        void init() {
            externalClassLoader = new ClassLoader[1];
            loader = mock(URLClassLoader.class);


        }

        @Test
        void testNormal() {
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.getScriptStandardizationEngineBefore("let a;", externalClassLoader, javaScriptFunctions, clientMongoOperator, logger, false)).thenCallRealMethod();
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(anyList(), any(ClientMongoOperator.class), any(Consumer.class), any(Log.class), anyBoolean())).thenAnswer(a -> {
                    Consumer<URLClassLoader> argument = (Consumer<URLClassLoader>)a.getArgument(2, Consumer.class);
                    argument.accept(loader);
                    return "buildInMethod";
                });
                Assertions.assertNotNull(ScriptStandardizationUtil.getScriptStandardizationEngineBefore("let a;", externalClassLoader, javaScriptFunctions, clientMongoOperator, logger, false));
                ssd.verify(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(anyList(), any(ClientMongoOperator.class), any(Consumer.class), any(Log.class), anyBoolean()));
            }
        }

        @Test
        void testScriptIsBlank() {
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.getScriptStandardizationEngineBefore(null, externalClassLoader, javaScriptFunctions, clientMongoOperator, logger, false)).thenCallRealMethod();
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(anyList(), any(ClientMongoOperator.class), any(Consumer.class), any(Log.class), anyBoolean())).thenAnswer(a -> {
                    Consumer<URLClassLoader> argument = (Consumer<URLClassLoader>)a.getArgument(2, Consumer.class);
                    argument.accept(loader);
                    return "buildInMethod";
                });
                Assertions.assertNotNull(ScriptStandardizationUtil.getScriptStandardizationEngineBefore(null, externalClassLoader, javaScriptFunctions, clientMongoOperator, logger, false));
                ssd.verify(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(anyList(), any(ClientMongoOperator.class), any(Consumer.class), any(Log.class), anyBoolean()));
            }
        }
    }

    @Nested
    class GetScriptStandardizationEngineAfterTest {
        ClassLoader[] externalClassLoader;
        ScriptFactory scriptFactory;
        ScriptEngine e;
        @BeforeEach
        void init() {
            externalClassLoader = new ClassLoader[1];
            scriptFactory = mock(ScriptFactory.class);
            e = mock(GraalJSScriptEngine.class);
            doNothing().when(e).put(anyString(), any(JsUtil.class));
            doNothing().when(e).put("tapLog", logger);
            doNothing().when(e).put(ScriptUtil.CACHE_SERVICE, memoryCacheGetter);
            doNothing().when(e).put("log", logger);
            when(scriptFactory.create(anyString(), any(ScriptOptions.class))).thenReturn(e);
        }

        @Test
        void testNormal() {
            try(MockedStatic<InstanceFactory> factory = mockStatic(InstanceFactory.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class);
                MockedStatic<ScriptUtil> su = mockStatic(ScriptUtil.class)) {
                factory.when(() -> InstanceFactory.instance(ScriptFactory.class, ScriptUtil.SCRIPT_FACTORY_TYPE)).thenReturn(scriptFactory);
                ssd.when(() -> ScriptStandardizationUtil.eval(any(ScriptEngine.class), anyString(), anyString())).then(a->null);
                ssd.when(() -> ScriptStandardizationUtil.getScriptStandardizationEngineAfter("jsEngineName", "script", externalClassLoader, memoryCacheGetter, logger, "scripts")).thenCallRealMethod();
                su.when(() -> ScriptUtil.evalImportSources(e,
                        "js/csvUtils.js",
                        "js/arrayUtils.js",
                        "js/dateUtils.js",
                        "js/exceptionUtils.js",
                        "js/stringUtils.js",
                        "js/mapUtils.js",
                        "js/log.js")).then(a->null);
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.getScriptStandardizationEngineAfter("jsEngineName", "script", externalClassLoader, memoryCacheGetter, logger, "scripts"));
                factory.verify(() -> InstanceFactory.instance(ScriptFactory.class, ScriptUtil.SCRIPT_FACTORY_TYPE), times(1));
                ssd.verify(() -> ScriptStandardizationUtil.eval(any(ScriptEngine.class), anyString(), anyString()), times(2));
                su.verify(() -> ScriptUtil.evalImportSources(e,
                        "js/csvUtils.js",
                        "js/arrayUtils.js",
                        "js/dateUtils.js",
                        "js/exceptionUtils.js",
                        "js/stringUtils.js",
                        "js/mapUtils.js",
                        "js/log.js"), times(1));
            }
            verify(scriptFactory, times(1)).create(anyString(), any(ScriptOptions.class));
            verify(e, times(1)).put(anyString(), any(JsUtil.class));
            verify(e, times(1)).put("tapLog", logger);
            verify(e, times(1)).put(ScriptUtil.CACHE_SERVICE, memoryCacheGetter);
            verify(e, times(1)).put("log", logger);
        }

    }

    @Nested
    class EvalTest {
        ScriptEngine e;
        @BeforeEach
        void init() throws ScriptException {
            e = mock(ScriptEngine.class);
            when(e.eval(anyString())).thenReturn("");
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.eval(e, "", ""));
        }
        @Test
        void testException() throws ScriptException {
            when(e.eval(anyString())).then(a -> {throw new ScriptException("");});
            Assertions.assertThrows(TapCodeException.class, () -> {
                try {
                    ScriptStandardizationUtil.eval(e, "", "");
                } catch (TapCodeException e) {
                    Assertions.assertEquals(ScriptProcessorExCode_30.GET_SCRIPT_STANDARDIZATION_ENGINE_FAILED, e.getCode());
                    throw e;
                }
            });
        }
    }

    @Nested
    class GetScriptStandardizationEngineTest {
        @Test
        void testNormal() {
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.getScriptStandardizationEngineBefore(anyString(), any(ClassLoader[].class), anyList(), any(ClientMongoOperator.class), any(Log.class), anyBoolean()))
                        .thenReturn("");
                ssd.when(() -> ScriptStandardizationUtil.getScriptStandardizationEngineAfter(anyString(), anyString(), any(ClassLoader[].class), any(ICacheGetter.class), any(Log.class), anyString()))
                        .thenReturn(mock(Invocable.class));
                ssd.when(() -> ScriptStandardizationUtil.getScriptStandardizationEngine("jsEngineName", "script", javaScriptFunctions, clientMongoOperator, memoryCacheGetter, logger, false)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.getScriptStandardizationEngine("jsEngineName", "script", javaScriptFunctions, clientMongoOperator, memoryCacheGetter, logger, false));
            }
        }
    }

    @Nested
    class InitStandardizationBuildInMethodTest {
        public static final String STANDARD_SCRIPT = "var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n" +
                "var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n" +
                "var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n" +
                "var HashMap = Java.type(\"java.util.HashMap\");\n" +
                "var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n" +
                "var ArrayList = Java.type(\"java.util.ArrayList\");\n" +
                "var uuid = UUIDGenerator.uuid;\n" +
                "var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n" +
                "var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n" +
                "var split_chinese = HanLPUtil.hanLPParticiple;\n" +
                "var util = Java.type(\"com.tapdata.processor.util.Util\");\n" +
                "var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n" +
                "var MD5 = function(str){return MD5Util.crypt(str, true);};\n" +
                "var Collections = Java.type(\"java.util.Collections\");\n" +
                "var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n" +
                "var sleep = function(ms){\n" +
                "var Thread = Java.type(\"java.lang.Thread\");\n" +
                "Thread.sleep(ms);\n" +
                "}\n";
        public static final String NOT_STANDARD_SCRIPT = "var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n" +
                "var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n" +
                "var httpUtil = Java.type(\"cn.hutool.http.HttpUtil\");\n" +
                "var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n" +
                "var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n";

        Consumer<URLClassLoader> consumer;
        @BeforeEach
        void init() {
            javaScriptFunctions.add(null);
            consumer = mock(Consumer.class);

            when(javaScriptFunction.isSystem()).thenReturn(false);
            doNothing().when(logger).debug(anyString(), anyList());
        }

        @Test
        void testNormal() {
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class);
                MockedStatic<ScriptUtil> su = mockStatic(ScriptUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList())).then(a->null);
                su.when(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList())).then(a->null);
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false)).thenCallRealMethod();
                String buildInMethod = ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false);
                Assertions.assertNotNull(buildInMethod);
                Assertions.assertEquals(STANDARD_SCRIPT + NOT_STANDARD_SCRIPT, buildInMethod);
                ssd.verify(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList()), times(1));
                su.verify(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList()), times(0));
            }
            verify(javaScriptFunction, times(1)).isSystem();
            verify(logger, times(0)).debug(anyString(), anyList());
        }

        @Test
        void testIsStandard() {
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class);
                MockedStatic<ScriptUtil> su = mockStatic(ScriptUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList())).then(a->null);
                su.when(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList())).then(a->null);
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, true)).thenCallRealMethod();
                String buildInMethod = ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, true);
                Assertions.assertNotNull(buildInMethod);
                Assertions.assertEquals(STANDARD_SCRIPT, buildInMethod);
                ssd.verify(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList()), times(0));
                su.verify(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList()), times(0));
            }
            verify(javaScriptFunction, times(0)).isSystem();
            verify(logger, times(0)).debug(anyString(), anyList());
        }

        @Test
        void testFunctionIsEmpty() {
            javaScriptFunctions = new ArrayList<>();
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class);
                MockedStatic<ScriptUtil> su = mockStatic(ScriptUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList())).then(a->null);
                su.when(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList())).then(a->null);
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false)).thenCallRealMethod();
                String buildInMethod = ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false);
                Assertions.assertNotNull(buildInMethod);
                Assertions.assertEquals(STANDARD_SCRIPT + NOT_STANDARD_SCRIPT, buildInMethod);
                ssd.verify(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList()), times(0));
                su.verify(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList()), times(0));
            }
            verify(javaScriptFunction, times(0)).isSystem();
            verify(logger, times(0)).debug(anyString(), anyList());
        }

        @Test
        void testIsSystem() {
            when(javaScriptFunction.isSystem()).thenReturn(true);
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class);
                MockedStatic<ScriptUtil> su = mockStatic(ScriptUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList())).then(a->null);
                su.when(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList())).then(a->null);
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false)).thenCallRealMethod();
                String buildInMethod = ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false);
                Assertions.assertNotNull(buildInMethod);
                Assertions.assertEquals(STANDARD_SCRIPT + NOT_STANDARD_SCRIPT, buildInMethod);
                ssd.verify(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList()), times(0));
                su.verify(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList()), times(0));
            }
            verify(javaScriptFunction, times(1)).isSystem();
            verify(logger, times(0)).debug(anyString(), anyList());
        }

        @Test
        void testUrlListNotEmpty() {
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class);
                MockedStatic<ScriptUtil> su = mockStatic(ScriptUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList())).then(a->{
                    ArrayList<URL> argument = (ArrayList<URL>) a.getArgument(3, ArrayList.class);
                    argument.add(mock(URL.class));
                    return null;
                });
                su.when(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList())).then(a->null);
                ssd.when(() -> ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false)).thenCallRealMethod();
                String buildInMethod = ScriptStandardizationUtil.initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, consumer, logger, false);
                Assertions.assertNotNull(buildInMethod);
                Assertions.assertEquals(STANDARD_SCRIPT + NOT_STANDARD_SCRIPT, buildInMethod);
                ssd.verify(() -> ScriptStandardizationUtil.initJSFunction(any(JavaScriptFunctions.class), any(StringBuilder.class), any(ClientMongoOperator.class), anyList()), times(1));
                su.verify(() -> ScriptUtil.urlClassLoader(any(Consumer.class), anyList()), times(1));
            }
            verify(javaScriptFunction, times(1)).isSystem();
            verify(logger, times(1)).debug(anyString(), anyList());
        }
    }

    @Nested
    class InitJSFunctionTest {
        List<URL> urlList;
        StringBuilder buildInMethod;
        AppType appType;
        Path filePath;
        URI uri;
        URL url;
        String tapdataWorkDir;

        @BeforeEach
        void init() throws MalformedURLException {
            urlList = new ArrayList<>();
            buildInMethod = new StringBuilder();
            appType = mock(AppType.class);
            filePath = mock(Path.class);
            uri = mock(URI.class);
            url = mock(URL.class);

            when(javaScriptFunction.getJSFunction()).thenReturn("name");
            when(javaScriptFunction.isJar()).thenReturn(true);
            when(appType.isDaas()).thenReturn(true);
            when(javaScriptFunction.getFileId()).thenReturn("FileId");

            when(filePath.toUri()).thenReturn(uri);
            when(uri.toURL()).thenReturn(url);
            when(filePath.toString()).thenReturn("");
            tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
        }
        @Test
        void testNormal() throws MalformedURLException {
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(true);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList));

                at.verify(AppType::currentType, times(1));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(1));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction), times(1));
                f.verify(() -> Files.notExists(filePath));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(1)).isJar();
            verify(appType, times(1)).isDaas();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(filePath, times(1)).toUri();
            verify(uri, times(1)).toURL();
        }

        @Test
        void testHttpClientMongoOperator() throws MalformedURLException {
            HttpClientMongoOperator httpClientMongoOperator = mock(HttpClientMongoOperator.class);
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(httpClientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(true);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, httpClientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, httpClientMongoOperator, urlList));
                at.verify(AppType::currentType, times(1));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(1));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(1));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(httpClientMongoOperator, filePath, javaScriptFunction), times(0));
                f.verify(() -> Files.notExists(filePath));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(1)).isJar();
            verify(appType, times(1)).isDaas();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(filePath, times(1)).toUri();
            verify(uri, times(1)).toURL();
        }

        @Test
        void testJsFunctionIsBlank() throws MalformedURLException {
            when(javaScriptFunction.getJSFunction()).thenReturn(null);
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(true);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList));
                at.verify(AppType::currentType, times(0));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction), times(0));
                f.verify(() -> Files.notExists(filePath), times(0));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(0)).isJar();
            verify(appType, times(0)).isDaas();
            verify(javaScriptFunction, times(0)).getFileId();
            verify(filePath, times(0)).toUri();
            verify(uri, times(0)).toURL();
        }

        @Test
        void testNotJar() throws MalformedURLException {
            when(javaScriptFunction.isJar()).thenReturn(false);
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(true);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList));
                at.verify(AppType::currentType, times(0));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction), times(0));
                f.verify(() -> Files.notExists(filePath), times(0));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(1)).isJar();
            verify(appType, times(0)).isDaas();
            verify(javaScriptFunction, times(0)).getFileId();
            verify(filePath, times(0)).toUri();
            verify(uri, times(0)).toURL();
        }

        @Test
        void testNotDaas() throws MalformedURLException {
            when(appType.isDaas()).thenReturn(false);
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(true);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList));
                at.verify(AppType::currentType, times(1));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction), times(0));
                f.verify(() -> Files.notExists(filePath), times(0));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(1)).isJar();
            verify(appType, times(1)).isDaas();
            verify(javaScriptFunction, times(0)).getFileId();
            verify(filePath, times(0)).toUri();
            verify(uri, times(0)).toURL();
        }

        @Test
        void testFilePathExists() throws MalformedURLException {
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(false);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList));
                at.verify(AppType::currentType, times(1));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(1));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction), times(0));
                f.verify(() -> Files.notExists(filePath), times(1));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(1)).isJar();
            verify(appType, times(1)).isDaas();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(filePath, times(0)).toUri();
            verify(uri, times(0)).toURL();
        }

        @Test
        void testToURLThrowException() throws MalformedURLException {
            when(uri.toURL()).then(a -> {
                throw new MalformedURLException("failed");
            });
            try(MockedStatic<AppType> at = mockStatic(AppType.class);
                MockedStatic<Paths> p = mockStatic(Paths.class);
                MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                at.when(AppType::currentType).thenReturn(appType);
                p.when(() -> Paths.get(tapdataWorkDir, "lib", "FileId")).thenReturn(filePath);
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class))).thenAnswer(a -> {return null;});
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenAnswer(a -> {return null;});
                f.when(() -> Files.notExists(filePath)).thenReturn(true);
                ssd.when(() -> ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList)).thenCallRealMethod();
                Assertions.assertThrows(TapCodeException.class, () -> {
                    try {
                        ScriptStandardizationUtil.initJSFunction(javaScriptFunction, buildInMethod, clientMongoOperator, urlList);
                    } catch (TapCodeException e) {
                        Assertions.assertEquals(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, e.getCode());
                        throw e;
                    }
                });

                at.verify(AppType::currentType, times(1));
                p.verify(() -> Paths.get(tapdataWorkDir, "lib", "FileId"), times(1));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(any(HttpClientMongoOperator.class), anyString(), any(Path.class)), times(0));
                ssd.verify(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction), times(1));
                f.verify(() -> Files.notExists(filePath));
            }
            verify(javaScriptFunction, times(1)).getJSFunction();
            verify(javaScriptFunction, times(1)).isJar();
            verify(appType, times(1)).isDaas();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(filePath, times(1)).toUri();
            verify(uri, times(1)).toURL();
        }
    }

    @Nested
    class DoWhenHttpClientMongoOperatorTest {
        Path filePath;
        File file;
        HttpClientMongoOperator httpClientMongoOperator;
        @BeforeEach
        void init() {
            file = mock(File.class);
            httpClientMongoOperator = mock(HttpClientMongoOperator.class);
            filePath = mock(Path.class);
            when(filePath.toString()).thenReturn("string");
        }
        @Test
        void testNormal() {
            when(httpClientMongoOperator.downloadFile(null, "file/" + "fileId", "string", true)).thenReturn(file);
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(httpClientMongoOperator, "fileId", filePath)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(httpClientMongoOperator, "fileId", filePath));
                verify(httpClientMongoOperator, times(1)).downloadFile(null, "file/" + "fileId", "string", true);
            }
        }

        @Test
        void testFileIsNull() {
            when(httpClientMongoOperator.downloadFile(null, "file/" + "fileId", "string", true)).thenReturn(null);
            try(MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenHttpClientMongoOperator(httpClientMongoOperator, "fileId", filePath)).thenCallRealMethod();
                Assertions.assertThrows(TapCodeException.class, () -> {
                    try {
                        ScriptStandardizationUtil.doWhenHttpClientMongoOperator(httpClientMongoOperator, "fileId", filePath);
                    } catch (TapCodeException e) {
                        Assertions.assertEquals(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, e.getCode());
                        throw e;
                    }
                });
                verify(httpClientMongoOperator, times(1)).downloadFile(null, "file/" + "fileId", "string", true);
            }
        }
    }

    @Nested
    class doWhenNotHttpClientMongoOperatorTest {
        Path filePath;
        Path parentPath;
        GridFSBucket gridFSBucket;
        GridFSDownloadStream gridFSDownloadStream;
        @BeforeEach
        void init() {
            filePath = mock(Path.class);
            parentPath = mock(Path.class);
            gridFSDownloadStream = mock(GridFSDownloadStream.class);
            gridFSBucket = mock(GridFSBucket.class);

            when(javaScriptFunction.getFileId()).thenReturn(new ObjectId().toHexString());
            when(gridFSBucket.openDownloadStream(any(ObjectId.class))).thenReturn(gridFSDownloadStream);
            when(clientMongoOperator.getGridFSBucket()).thenReturn(gridFSBucket);
            when(filePath.getParent()).thenReturn(parentPath);
            when(filePath.toString()).thenReturn("path-string");
        }
        @Test
        void testNormal() {
            try(MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenCallRealMethod();
                f.when(() -> Files.notExists(parentPath)).thenReturn(true);
                f.when(() -> Files.createDirectories(parentPath)).thenReturn(mock(Path.class));
                f.when(() -> Files.createFile(filePath)).thenReturn(mock(Path.class));
                f.when(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING)).thenReturn(0L);
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction));
                f.verify(() -> Files.notExists(parentPath), times(1));
                f.verify(() -> Files.createDirectories(parentPath), times(1));
                f.verify(() -> Files.createFile(filePath), times(1));
                f.verify(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING), times(1));
            }
            verify(clientMongoOperator, times(1)).getGridFSBucket();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(gridFSBucket, times(1)).openDownloadStream(any(ObjectId.class));
            verify(filePath, times(2)).getParent();
        }

        @Test
        void testNotExists() {
            try(MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenCallRealMethod();
                f.when(() -> Files.notExists(parentPath)).thenReturn(false);
                f.when(() -> Files.createDirectories(parentPath)).thenReturn(mock(Path.class));
                f.when(() -> Files.createFile(filePath)).thenReturn(mock(Path.class));
                f.when(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING)).thenReturn(0L);
                Assertions.assertDoesNotThrow(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction));
                f.verify(() -> Files.notExists(parentPath), times(1));
                f.verify(() -> Files.createDirectories(parentPath), times(0));
                f.verify(() -> Files.createFile(filePath), times(1));
                f.verify(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING), times(1));
            }
            verify(clientMongoOperator, times(1)).getGridFSBucket();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(gridFSBucket, times(1)).openDownloadStream(any(ObjectId.class));
            verify(filePath, times(1)).getParent();
        }

        @Test
        void testIOExceptionWhenCreateDirectories() {
            try(MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenCallRealMethod();
                f.when(() -> Files.notExists(parentPath)).thenReturn(true);
                f.when(() -> Files.createDirectories(parentPath)).then(a-> {throw new IOException("exception");});
                f.when(() -> Files.createFile(filePath)).thenReturn(mock(Path.class));
                f.when(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING)).thenReturn(0L);
                Assertions.assertThrows(TapCodeException.class, () -> {
                    try {
                        ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction);
                    } catch (TapCodeException e) {
                        Assertions.assertEquals(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, e.getCode());
                        throw e;
                    }
                });
                f.verify(() -> Files.notExists(parentPath), times(1));
                f.verify(() -> Files.createDirectories(parentPath), times(1));
                f.verify(() -> Files.createFile(filePath), times(0));
                f.verify(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING), times(0));
            }
            verify(clientMongoOperator, times(1)).getGridFSBucket();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(gridFSBucket, times(1)).openDownloadStream(any(ObjectId.class));
            verify(filePath, times(2)).getParent();
        }
        @Test
        void testIOExceptionWhenCreateFile() {
            try(MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenCallRealMethod();
                f.when(() -> Files.notExists(parentPath)).thenReturn(true);
                f.when(() -> Files.createDirectories(parentPath)).thenReturn(mock(Path.class));
                f.when(() -> Files.createFile(filePath)).then(a-> {throw new IOException("exception");});
                f.when(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING)).thenReturn(0L);
                Assertions.assertThrows(TapCodeException.class, () -> {
                    try {
                        ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction);
                    } catch (TapCodeException e) {
                        Assertions.assertEquals(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, e.getCode());
                        throw e;
                    }
                });
                f.verify(() -> Files.notExists(parentPath), times(1));
                f.verify(() -> Files.createDirectories(parentPath), times(1));
                f.verify(() -> Files.createFile(filePath), times(1));
                f.verify(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING), times(0));
            }
            verify(clientMongoOperator, times(1)).getGridFSBucket();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(gridFSBucket, times(1)).openDownloadStream(any(ObjectId.class));
            verify(filePath, times(2)).getParent();
        }
        @Test
        void testIOExceptionWhenCopy() {
            try(MockedStatic<Files> f = mockStatic(Files.class);
                MockedStatic<ScriptStandardizationUtil> ssd = mockStatic(ScriptStandardizationUtil.class)) {
                ssd.when(() -> ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction)).thenCallRealMethod();
                f.when(() -> Files.notExists(parentPath)).thenReturn(true);
                f.when(() -> Files.createDirectories(parentPath)).thenReturn(mock(Path.class));
                f.when(() -> Files.createFile(filePath)).thenReturn(mock(Path.class));
                f.when(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING)).then(a -> {throw new IOException("exception");});
                Assertions.assertThrows(TapCodeException.class, () -> {
                    try {
                        ScriptStandardizationUtil.doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction);
                    } catch (TapCodeException e) {
                        Assertions.assertEquals(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, e.getCode());
                        throw e;
                    }
                });
                f.verify(() -> Files.notExists(parentPath), times(1));
                f.verify(() -> Files.createDirectories(parentPath), times(1));
                f.verify(() -> Files.createFile(filePath), times(1));
                f.verify(() -> Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING), times(1));
            }
            verify(clientMongoOperator, times(1)).getGridFSBucket();
            verify(javaScriptFunction, times(1)).getFileId();
            verify(gridFSBucket, times(1)).openDownloadStream(any(ObjectId.class));
            verify(filePath, times(2)).getParent();
        }
    }
}