package io.tapdata.js.connector.iengine;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.util.ScriptUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.js.connector.enums.Constants;
import io.tapdata.js.utils.Collector;

import javax.script.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;


public class LoadJavaScripter {
    private static final String TAG = LoadJavaScripter.class.getSimpleName();
    private static final String EVAL_ID = ScriptEngine.FILENAME;

    public static final String NASHORN_ENGINE = "nashorn";
    public static final String GRAAL_ENGINE = "graal.js";
    private boolean hasLoadBaseJs = false;
    private boolean hasLoadJs = false;

    private String jarFilePath;
    private String flooder;
    private ScriptEngine scriptEngine;

    public ScriptEngine scriptEngine() {
        return this.scriptEngine;
    }

    private String ENGINE_TYPE = GRAAL_ENGINE;

    public LoadJavaScripter params(String jarFilePath, String flooder) {
        this.jarFilePath = jarFilePath;
        this.flooder = flooder;
        return this;
    }

    public boolean hasLoad() {
        return this.hasLoadJs;
    }

    public void reload() {
        this.hasLoadJs = false;
    }

    public static LoadJavaScripter loader(String jarFilePath, String flooder) {
        LoadJavaScripter loadJavaScripter = new LoadJavaScripter();
        return loadJavaScripter.params(jarFilePath, flooder).init();
    }

    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");

    public LoadJavaScripter init() {
        this.scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(ENGINE_TYPE));
        try {
            StringBuilder buildInMethod = new StringBuilder();
            //buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
            //buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
            //buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
            buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
            buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
            buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
            //buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
            //buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
            //buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
            //buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
            //buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
            //buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
            //buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
            buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
            //buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
            buildInMethod.append("var sleep = function(ms){\n" +
                    "var Thread = Java.type(\"java.lang.Thread\");\n" +
                    "Thread.sleep(ms);\n" +
                    "}\n");
            this.scriptEngine.eval(buildInMethod.toString());
        }catch (Exception e) {
            TapLogger.warn(TAG, "Can't evel default util to engine.");
        }

        return this;
    }

    public synchronized ScriptEngine load(Enumeration<URL> resources) {
        List<URL> list = new ArrayList<>();
        while (resources.hasMoreElements()) {
            list.add(resources.nextElement());
        }
        if (!this.hasLoadBaseJs) {
            try {
                for (URL url : list) {
                    List<Map.Entry<InputStream, File>> files = this.javaFiles(url, null, "io/tapdata/js-core");
//                    for (Map.Entry<InputStream, File> file : files) {
//                        this.scriptEngine.eval(ScriptUtil.fileToString(file.getKey()));
//                    }
                    StringBuilder builder = new StringBuilder();
                    for (Map.Entry<InputStream, File> file : files) {
                        builder.append(ScriptUtil.fileToString(file.getKey())).append("\n\n");
                    }
                    this.scriptEngine.eval(builder.toString());
                }
                this.hasLoadBaseJs = true;
            } catch (Exception e) {
                TapLogger.warn(TAG, String.format("Unable to load configuration javascript to jsEngine. %s.", e.getMessage()));
            }
        }
        if (!this.hasLoadJs) {
            try {
                for (URL url : list) {
                    List<Map.Entry<InputStream, File>> files = this.javaScriptFiles(url);
//                    for (Map.Entry<InputStream, File> file : files) {
//                        this.scriptEngine.eval(ScriptUtil.fileToString(file.getKey()));
//                    }
                    StringBuilder builder = new StringBuilder();
                    for (Map.Entry<InputStream, File> file : files) {
                        builder.append(ScriptUtil.fileToString(file.getKey())).append("\n\n");
                    }
                    this.scriptEngine.eval(builder.toString());
                }
                this.hasLoadJs = true;
                return this.scriptEngine;
            } catch (Exception error) {
                throw new CoreException("Error java script code, message: " + error.getMessage());
            }
        }
        return this.scriptEngine;
    }

    private List<Map.Entry<InputStream, File>> javaFiles(URL url, String flooder, String fileFlooder) {
        List<Map.Entry<InputStream, File>> fileList = new ArrayList<>();
        String path = url.getPath();
        try {
            List<Map.Entry<InputStream, File>> collect = getAllFileFromJar(path, Optional.ofNullable(flooder).orElse(this.flooder), Optional.ofNullable(fileFlooder).orElse(this.flooder));
            fileList.addAll(collect);
        } catch (Exception ignored) {
            throw new CoreException(String.format("Unable to get the file list, the file directory is: %s. ", path));
        }
        return fileList;
    }

    private List<Map.Entry<InputStream, File>> javaScriptFiles(URL url, String flooder, String fileFlooder) {
        Map.Entry<InputStream, File> connectorFile = null;
        List<Map.Entry<InputStream, File>> fileList = new ArrayList<>();
        String path = url.getPath();
        try {
            List<Map.Entry<InputStream, File>> collect = getAllFileFromJar(path, Optional.ofNullable(flooder).orElse(this.flooder), Optional.ofNullable(flooder).orElse(this.flooder));
            for (Map.Entry<InputStream, File> entry : collect) {
                File file = entry.getValue();
                if (this.fileIsConnectorJs(file)) {
                    connectorFile = entry;
                    //this.getSupportFunctions(entry.getKey());
                } else {
                    fileList.add(entry);
                }
            }
        } catch (Exception ignored) {
            throw new CoreException(String.format("Unable to get the file list, the file directory is: %s. ", path));
        }
        if (Objects.isNull(connectorFile)) {
            throw new CoreException("You must use connector.js as the entry of the data source. Please create a connector.js file and implement the data source method in this article.");
        }
        fileList.add(connectorFile);
        return fileList;
    }

    //根据父路径加载全部JS文件并返回
    //connector.js必须放在最后
    //不存在connector.js就报错
    private List<Map.Entry<InputStream, File>> javaScriptFiles(URL url) {
        return this.javaScriptFiles(url, null, null);
    }

    private List<Map.Entry<InputStream, File>> getAllFileFromJar(String path, String flooder, String fileFlooder) {
        List<Map.Entry<InputStream, File>> fileList = new ArrayList<>();
        String pathJar = Objects.nonNull(jarFilePath) && !"".equals(jarFilePath) ? jarFilePath : path.replace("file:/", "/").replace("!/" + flooder, "");
        try {
            List<Map.Entry<ZipEntry, InputStream>> collect =
                    readJarFile(new JarFile(pathJar), fileFlooder).collect(Collectors.toList());
            for (Map.Entry<ZipEntry, InputStream> entry : collect) {
                String key = entry.getKey().getName();
                InputStream stream = entry.getValue();
                fileList.add(new AbstractMap.SimpleEntry<InputStream, File>(stream, new File(key)));
            }
        } catch (IOException e) {
        }
        return fileList;
    }

    public static Stream<Map.Entry<ZipEntry, InputStream>> readJarFile(JarFile jarFile, String prefix) {
        Stream<Map.Entry<ZipEntry, InputStream>> readingStream =
                jarFile.stream().filter(entry -> !entry.isDirectory() && entry.getName().startsWith(prefix))
                        .map(entry -> {
                            try {
                                return new AbstractMap.SimpleEntry<>(entry, jarFile.getInputStream(entry));
                            } catch (IOException e) {
                                return new AbstractMap.SimpleEntry<>(entry, null);
                            }
                        });
        return readingStream.onClose(() -> {
            try {
                jarFile.close();
            } catch (IOException e) {
            }
        });
    }

    //判断文件是否connector.js
    private boolean fileIsConnectorJs(File file) {
        return Objects.nonNull(file) && Constants.CONNECTOR_JS_NAME.equals(file.getName());
    }

    public boolean functioned(String functionName) {
        if (Objects.isNull(functionName) || Objects.isNull(this.scriptEngine)) return false;
        try {
            //Invocable invocable = (Invocable) this.scriptEngine;
            //invocable.invokeFunction(functionName);
            Object o = this.scriptEngine.eval(functionName);
            return Objects.nonNull(o) && o instanceof Function;
        } catch (Exception ignored) {
            return false;
        }
    }

    private void binding(String key, Object name, int scope) {
        Bindings bindings = this.scriptEngine.getBindings(scope);
        bindings.put(key, name);
    }

    public void put(String key, Object javaValue) {
        Optional.ofNullable(this.scriptEngine).ifPresent(e -> e.put(key, javaValue));
    }

    public void bindingGlobal(String key, Object binder) {
        this.binding(key, binder, ScriptContext.GLOBAL_SCOPE);
    }

    public void bindingEngine(String key, Object binder) {
        this.binding(key, binder, ScriptContext.ENGINE_SCOPE);
    }

    public Object invoker(String functionName, Object... params) {
        switch (ENGINE_TYPE) {
            case NASHORN_ENGINE:
                return invokerNashorn(functionName, params);
            case GRAAL_ENGINE:
                return invokerGraal(functionName, params);
        }
        return null;
    }

    public Object invokerNashorn(String functionName, Object... params) {
        if (Objects.isNull(functionName)) return null;
        //AtomicReference<Throwable> scriptException = new AtomicReference<>();
        if (Objects.isNull(this.scriptEngine)) return null;
        Function<Object[], Object> polyglotMapAndFunction;
        try {
            //Invocable invocable = (Invocable) this.scriptEngine;
//            Object apply = invocable.invokeFunction(functionName, params);
            polyglotMapAndFunction = (Function<Object[], Object>) this.scriptEngine.get(functionName);

            Object apply = polyglotMapAndFunction.apply(params);
            if (Objects.isNull(apply)) {
                return null;
            } else if (apply instanceof Map || apply instanceof Collection) {
                try {
                    String toString = apply.toString();
                    if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                        toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                    }
                    return ConnectorBase.fromJsonArray(toString);
                } catch (Exception e) {
                    try {
                        String string = apply.toString();
                        return "{}".equals(string) ? new HashMap<>() : fromJson(string);
                    } catch (Exception error) {
                        TapLogger.warn(TAG, "function named " + functionName + " exec failed, function return value is: " + apply.toString() + "error cast java Object.");
                        return null;
                    }
                }
            } else {
                return apply;
            }
        } catch (Exception e) {
            throw new CoreException(String.format(" Method execution failed, method name -[ %s ], params are -[ %s ], message: %s", functionName, toJson(params), e.getMessage()));
        }
    }

    public synchronized Object invokerGraal(String functionName, Object... params) {
        if (Objects.isNull(functionName)) return null;
        if (Objects.isNull(this.scriptEngine)) return null;
        //Function<Object[], Object> polyglotMapAndFunction;
        //Object connectionConfig = this.scriptEngine.get(ExecuteConfig.CONNECTION_CONFIG);
        //if (Objects.nonNull(connectionConfig) && connectionConfig instanceof Map){
        //}
        //Object nodeConfigMap = this.scriptEngine.get(ExecuteConfig.NODE_CONFIG);
        try {
            Invocable invocable = (Invocable) this.scriptEngine;
            Object apply = invocable.invokeFunction(functionName, params);
            return LoadJavaScripter.covertData(apply);
        } catch (Exception e) {
            throw new CoreException(String.format("JavaScript Method execution failed, method name -[%s], params are -[%s], message: %s, %s", functionName, toJson(params), e.getMessage(), InstanceFactory.instance(TapUtils.class).getStackTrace(e)));
        }
    }


    public static Object covertData(Object apply) {
        if (Objects.isNull(apply)) {
            return null;
        } else if (apply instanceof Map) {
            return InstanceFactory.instance(TapUtils.class).cloneMap((Map<String, Object>) apply);//fromJson(toJson(apply));
        } else if (apply instanceof Collection) {
            try {
                return new ArrayList<>((List<Object>) apply);//ConnectorBase.fromJsonArray(toJson(apply));
            } catch (Exception e) {
                String toString = apply.toString();
                if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                    toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                }
                return ConnectorBase.fromJsonArray(toString);
            }
        } else{
            return apply;
        }
    }
}
