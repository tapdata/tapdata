package com.tapdata.processor.standard;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 标准化JS节点初始化工具
 * @author Gavin'xiao
 * */
public class ScriptStandardizationUtil {
    private ScriptStandardizationUtil() {
    }

    protected static String getScriptStandardizationEngineBefore(String script,
                                                                 ClassLoader[] externalClassLoader,
                                                                 List<JavaScriptFunctions> javaScriptFunctions,
                                                                 ClientMongoOperator clientMongoOperator,
                                                                 Log logger,
                                                                 boolean standard) {
        if (StringUtils.isBlank(script)) {
            script = "function process(record){\n\treturn record;\n}";
        }
        String buildInMethod = initStandardizationBuildInMethod(javaScriptFunctions, clientMongoOperator, urlClassLoader -> externalClassLoader[0] = urlClassLoader, logger, standard);
        return script + System.lineSeparator() + buildInMethod;
    }

    protected static Invocable getScriptStandardizationEngineAfter(
            String jsEngineName,
            String script,
            ClassLoader[] externalClassLoader,
            ICacheGetter memoryCacheGetter,
            Log logger,
            String scripts) {
        final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, ScriptUtil.SCRIPT_FACTORY_TYPE);
        ScriptOptions options = new ScriptOptions()
                .engineName(jsEngineName)
                .classLoader(externalClassLoader[0])
                .log(logger);
        ScriptEngine e = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, options);
        e.put("tapUtil", new JsUtil());
        e.put("tapLog", logger);
        eval(e, "tapLog.info('Init standardized JS engine');", String.format("Can not init standardized JS engine: %s, script eval %s error", script, e));
        ScriptUtil.evalImportSources(e,
                "js/csvUtils.js",
                "js/arrayUtils.js",
                "js/dateUtils.js",
                "js/exceptionUtils.js",
                "js/stringUtils.js",
                "js/mapUtils.js",
                "js/log.js");
        eval(e, scripts, String.format("Incorrect JS code, syntax error found: %s, script eval %s error, please check your javascript code", scripts, e));
        Optional.ofNullable(memoryCacheGetter).ifPresent(s -> e.put(ScriptUtil.CACHE_SERVICE, s));
        Optional.ofNullable(logger).ifPresent(s -> e.put("log", s));
        return (Invocable) e;
    }

    protected static void eval(ScriptEngine e, String script, String failedMessage) {
        try {
            e.eval(script);
        } catch (ScriptException es){
            throw new TapCodeException(ScriptProcessorExCode_30.GET_SCRIPT_STANDARDIZATION_ENGINE_FAILED, failedMessage, es);
        }
    }

    public static Invocable getScriptStandardizationEngine(
            String jsEngineName,
            String script,
            List<JavaScriptFunctions> javaScriptFunctions,
            ClientMongoOperator clientMongoOperator,
            ICacheGetter memoryCacheGetter,
            Log logger,
            boolean standard ) {
        AcceptClassLoader acceptClassLoader = new AcceptClassLoader() {
            @Override
            public String before(ClassLoader[] externalClassLoader) {
                return getScriptStandardizationEngineBefore(script, externalClassLoader, javaScriptFunctions, clientMongoOperator, logger, standard);
            }

            @Override
            public Invocable after(String before, ClassLoader[] externalClassLoader) {
                return getScriptStandardizationEngineAfter(jsEngineName, script, externalClassLoader, memoryCacheGetter, logger, before);
            }
        };
        return acceptClassLoader.doAccept();
    }

    protected static String initStandardizationBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions,
                                                          ClientMongoOperator clientMongoOperator,
                                                          Consumer<URLClassLoader> consumer,
                                                          Log logger,
                                                          boolean standard) {
        StringBuilder buildInMethod = new StringBuilder();

        //Expired, will be ignored in the near future
        buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
        buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
        buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
        buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
        buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
        buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
        buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
        buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
        buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
        buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
        buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
        buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
        buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
        buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
        buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
        buildInMethod.append("var sleep = function(ms){\n" +
                "var Thread = Java.type(\"java.lang.Thread\");\n" +
                "Thread.sleep(ms);\n" +
                "}\n");
        if (standard) {
            return buildInMethod.toString();
        }
        buildInMethod.append("var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n");
        buildInMethod.append("var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n");
        buildInMethod.append("var httpUtil = Java.type(\"cn.hutool.http.HttpUtil\");\n");
        buildInMethod.append("var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n");
        buildInMethod.append("var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n");

        if (!CollectionUtils.isNotEmpty(javaScriptFunctions)) {
            return buildInMethod.toString();
        }
        List<URL> urlList = new ArrayList<>();
        javaScriptFunctions.stream()
                .filter(f -> Objects.nonNull(f) && !f.isSystem())
                .forEach(f -> initJSFunction(f, buildInMethod, clientMongoOperator, urlList));
        if (!urlList.isEmpty()) {
            logger.debug("urlClassLoader will load: {}", urlList);
            ScriptUtil.urlClassLoader(consumer,urlList);
        }
        return buildInMethod.toString();
    }

    protected static void initJSFunction(JavaScriptFunctions javaScriptFunction,
                                         StringBuilder buildInMethod,
                                         ClientMongoOperator clientMongoOperator,
                                         List<URL> urlList) {
        String jsFunction = javaScriptFunction.getJSFunction();
        if (!StringUtils.isNotBlank(jsFunction)) {
            return;
        }
        buildInMethod.append(jsFunction).append("\n");
        if (!(javaScriptFunction.isJar() && AppType.currentType().isDaas())) {
            return;
        }
        //定义类加载器
        String fileId = javaScriptFunction.getFileId();
        final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", fileId);
        if (!Files.notExists(filePath)) {
            return;
        }
        if (clientMongoOperator instanceof HttpClientMongoOperator) {
            doWhenHttpClientMongoOperator((HttpClientMongoOperator)clientMongoOperator, fileId, filePath);
        } else {
            doWhenNotHttpClientMongoOperator(clientMongoOperator, filePath, javaScriptFunction);
        }
        try {
            URL url = filePath.toUri().toURL();
            urlList.add(url);
        } catch (MalformedURLException e) {
            throw new TapCodeException(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, String.format("create function jar file %s", filePath),e);
        }
    }

    protected static void doWhenHttpClientMongoOperator(HttpClientMongoOperator clientMongoOperator,
                                                        String fileId,
                                                        Path filePath) {
        File file = clientMongoOperator.downloadFile(null, "file/" + fileId, filePath.toString(), true);
        if (null == file) {
            throw new TapCodeException(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, String.format("file not found,fileId:%s,filePath:%s", fileId, filePath));
        }
    }

    protected static void doWhenNotHttpClientMongoOperator(ClientMongoOperator clientMongoOperator,
                                                           Path filePath,
                                                           JavaScriptFunctions javaScriptFunction) {
        GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
        try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(javaScriptFunction.getFileId()))) {
            if (Files.notExists(filePath.getParent())) {
                Files.createDirectories(filePath.getParent());
            }
            Files.createFile(filePath);
            Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new TapCodeException(ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED, String.format("create function jar file %s", filePath),e);
        }
    }
}
