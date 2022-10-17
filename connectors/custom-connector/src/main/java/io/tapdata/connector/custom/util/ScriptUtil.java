package io.tapdata.connector.custom.util;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import io.tapdata.connector.custom.bean.ClientMongoOperator;
import io.tapdata.connector.custom.bean.JavaScriptFunctions;
import io.tapdata.connector.custom.constant.JSEngineEnum;
import io.tapdata.constant.AppType;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;
import org.bson.types.ObjectId;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
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

public class ScriptUtil {

    /**
     * script
     */
    public static final String FUNCTION_PREFIX = "function ";
    public static final String SOURCE_FUNCTION_NAME = "requestData";
    public static final String BEFORE_FUNCTION_NAME = "before";
    public static final String AFTER_FUNCTION_NAME = "after";
    public static final String TARGET_FUNCTION_NAME = "onData";
    public static final String FUNCTION_SUFFIX = "\n}";
    public static final String INITIAL_FUNCTION_IN_PARAM = " () {\n";
    public static final String CDC_FUNCTION_IN_PARAM = " (ctx) {\n";
    public static final String TARGET_FUNCTION_IN_PARAM = " (data) {\n";

    private static final String TAG = ScriptUtil.class.getSimpleName();

    public static ScriptEngine initScriptEngine(String jsEngineName) {
        JSEngineEnum jsEngineEnum = JSEngineEnum.getByEngineName(jsEngineName);
        ScriptEngine scriptEngine;
        if (jsEngineEnum == JSEngineEnum.GRAALVM_JS) {
            scriptEngine = GraalJSScriptEngine
                    .create(null,
                            Context.newBuilder("js")
                                    .allowAllAccess(true)
                                    .allowHostAccess(HostAccess.newBuilder(HostAccess.ALL)
                                            .targetTypeMapping(Value.class, Object.class
                                                    , v -> v.hasArrayElements() && v.hasMembers()
                                                    , v -> v.as(List.class)
                                            ).build()
                                    )
                    );
        } else {
            scriptEngine = new ScriptEngineManager().getEngineByName(jsEngineEnum.getEngineName());
        }
        return scriptEngine;
    }

    public static String appendSourceFunctionScript(String script, boolean isInitial) {
        script = FUNCTION_PREFIX +
                SOURCE_FUNCTION_NAME +
                (isInitial ? INITIAL_FUNCTION_IN_PARAM : CDC_FUNCTION_IN_PARAM) +
                script +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String appendTargetFunctionScript(String script) {
        script = FUNCTION_PREFIX +
                TARGET_FUNCTION_NAME +
                TARGET_FUNCTION_IN_PARAM +
                script +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String appendBeforeFunctionScript(String script) {
        script = FUNCTION_PREFIX +
                BEFORE_FUNCTION_NAME +
                INITIAL_FUNCTION_IN_PARAM +
                (EmptyKit.isEmpty(script) ? "" : script) +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String appendAfterFunctionScript(String script) {
        script = FUNCTION_PREFIX +
                AFTER_FUNCTION_NAME +
                INITIAL_FUNCTION_IN_PARAM +
                script +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String initBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator) {
        StringBuilder buildInMethod = new StringBuilder();
        buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
        buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
        buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
        buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
        buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
        buildInMethod.append("var Date = Java.type(\"java.util.Date\");\n");
        buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
        buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
        buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
        buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
        buildInMethod.append("var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n");
        buildInMethod.append("var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n");
        buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
        buildInMethod.append("var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n");
        buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
        buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
        buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
        buildInMethod.append("var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n");
        buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
        buildInMethod.append("var sleep = function(ms){\n" +
                "var Thread = Java.type(\"java.lang.Thread\");\n" +
                "Thread.sleep(ms);\n" +
                "}\n");

        if (EmptyKit.isNotEmpty(javaScriptFunctions)) {
            List<URL> urlList = new ArrayList<>();
            for (JavaScriptFunctions javaScriptFunction : javaScriptFunctions) {
                if (javaScriptFunction.isSystem()) {
                    continue;
                }
                String jsFunction = javaScriptFunction.getJSFunction();
                if (EmptyKit.isNotBlank(jsFunction)) {
                    buildInMethod.append(jsFunction).append("\n");
                    if (javaScriptFunction.isJar() && AppType.init().isDaas()) {
                        final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", javaScriptFunction.getFileId());
                        if (Files.notExists(filePath)) {
                            GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
                            try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(javaScriptFunction.getFileId()))) {
                                if (Files.notExists(filePath.getParent())) {
                                    Files.createDirectories(filePath.getParent());
                                }
                                Files.createFile(filePath);
                                Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
                            } catch (IOException e) {
                                TapLogger.error(TAG, "create file error ", e);
                            }
                        }
                        try {
                            URL url = filePath.toUri().toURL();
                            urlList.add(url);
                        } catch (MalformedURLException e) {
                            TapLogger.error(TAG, "add url error", e);
                        }
                    }
                }
            }
            if (EmptyKit.isNotEmpty(urlList)) {
                TapLogger.debug(TAG, "urlClassLoader will load: {}", urlList);
                final URLClassLoader urlClassLoader = new ScriptUtil.CustomerClassLoader(urlList.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
                Thread.currentThread().setContextClassLoader(urlClassLoader);
            }
        }

        return buildInMethod.toString();
    }

    public static class CustomerClassLoader extends URLClassLoader {
        public CustomerClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }
    }
}

