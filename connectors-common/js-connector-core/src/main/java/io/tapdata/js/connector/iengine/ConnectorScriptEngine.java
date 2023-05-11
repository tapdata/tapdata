package io.tapdata.js.connector.iengine;

import io.tapdata.entity.script.ScriptOptions;

import javax.script.*;
import java.io.Reader;
import java.net.URLClassLoader;

public class ConnectorScriptEngine implements ScriptEngine, Invocable {

    private final ScriptEngine scriptEngine;
    private final Invocable invocable;
    private final String buildInScript = "";
    private URLClassLoader externalJarClassLoader;

    public ConnectorScriptEngine(ScriptOptions scriptOptions) {
        ScriptEngineManager engineManager = new ScriptEngineManager();
        this.scriptEngine = engineManager.getEngineByName("javascript");
        invocable = (Invocable) scriptEngine;
        ScriptEngineInit.create().init(this.scriptEngine);
    }

    public ConnectorScriptEngine(ScriptEngine scriptEngine) {
        this.scriptEngine = scriptEngine;
        invocable = (Invocable) scriptEngine;
        //ScriptEngineInit.create().init(this.scriptEngine);
    }

    private String combineFunctions(String script) {
        return buildInScript + "\n" + script;
    }

    @Override
    public Object invokeMethod(Object thiz, String name, Object... args) throws ScriptException, NoSuchMethodException {
        return invocable.invokeMethod(thiz, name, args);
    }

    @Override
    public Object invokeFunction(String name, Object... args) throws ScriptException, NoSuchMethodException {
        return invocable.invokeFunction(name, args);
    }

    @Override
    public <T> T getInterface(Class<T> clasz) {
        return invocable.getInterface(clasz);
    }

    @Override
    public <T> T getInterface(Object thiz, Class<T> clasz) {
        return invocable.getInterface(thiz, clasz);
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return scriptEngine.eval(combineFunctions(script), context);
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object eval(String script) throws ScriptException {
        return scriptEngine.eval(combineFunctions(script));
    }

    @Override
    public Object eval(Reader reader) throws ScriptException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object eval(String script, Bindings n) throws ScriptException {
        return scriptEngine.eval(combineFunctions(script), n);
    }

    @Override
    public Object eval(Reader reader, Bindings n) throws ScriptException {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void put(String key, Object value) {
        scriptEngine.put(key, value);
    }

    @Override
    public Object get(String key) {
        return scriptEngine.get(key);
    }

    @Override
    public Bindings getBindings(int scope) {
        return scriptEngine.getBindings(scope);
    }

    @Override
    public void setBindings(Bindings bindings, int scope) {
        scriptEngine.setBindings(bindings, scope);
    }

    @Override
    public Bindings createBindings() {
        return scriptEngine.createBindings();
    }

    @Override
    public ScriptContext getContext() {
        return scriptEngine.getContext();
    }

    @Override
    public void setContext(ScriptContext context) {
        scriptEngine.setContext(context);
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return scriptEngine.getFactory();
    }

    //        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
//        List<JavaScriptFunctions> javaScriptFunctions = JobUtil.getJavaScriptFunctions(clientMongoOperator);
//        this.buildInScript = ScriptUtil.initBuildInMethod(javaScriptFunctions, clientMongoOperator, urlClassLoader -> externalJarClassLoader = urlClassLoader);
    //this.scriptEngine = ScriptFactory.create("javascript",scriptOptions);

//        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
//        this.scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().customEngine(ScriptFactory.TYPE_JAVASCRIPT, ConnectorScriptEngine.class));

    //        if (CollectionUtils.isNotEmpty(javaScriptFunctions)) {
//            List<URL> urlList = new ArrayList<>();
//            for (JavaScriptFunctions javaScriptFunction : javaScriptFunctions) {
//                if (javaScriptFunction.isSystem()) {
//                    continue;
//                }
//                String jsFunction = javaScriptFunction.getJSFunction();
//                if (StringUtils.isNotBlank(jsFunction)) {
//                    buildInMethod.append(jsFunction).append("\n");
//                    if (javaScriptFunction.isJar() && AppType.init().isDaas()) {
//                        //定义类加载器
//                        String fileId = javaScriptFunction.getFileId();
//                        final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", fileId);
//                        if (Files.notExists(filePath)) {
//                            if (clientMongoOperator instanceof HttpClientMongoOperator) {
//                                File file = ((HttpClientMongoOperator) clientMongoOperator).downloadFile(null, "file/" + fileId, filePath.toString(), true);
//                                if (null == file) {
//                                    throw new RuntimeException("not found");
//                                }
//                            } else {
//                                GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
//                                try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(javaScriptFunction.getFileId()))) {
//                                    if (Files.notExists(filePath.getParent())) {
//                                        Files.createDirectories(filePath.getParent());
//                                    }
//                                    Files.createFile(filePath);
//                                    Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
//                                } catch (Exception e) {
//                                    throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
//                                }
//                            }
//                        }
//                        try {
//                            URL url = filePath.toUri().toURL();
//                            urlList.add(url);
//                        } catch (Exception e) {
//                            throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
//                        }
//                    }
//                }
//            }
//            if (CollectionUtils.isNotEmpty(urlList)) {
//                logger.debug("urlClassLoader will load: {}", urlList);
////				final URLClassLoader urlClassLoader = new CustomerClassLoader(urlList.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
//                final URLClassLoader urlClassLoader = new CustomerClassLoader(urlList.toArray(new URL[0]), ScriptUtil.class.getClassLoader());
//                if(consumer != null) {
//                    consumer.accept(urlClassLoader);
//                }
////				Thread.currentThread().setContextClassLoader(urlClassLoader);
//            }
//        }

//        this.buildInScript = "var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n" +
//                "var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n" +
//                "var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n" +
//                "var HashMap = Java.type(\"java.util.HashMap\");\n" +
//                "var ArrayList = Java.type(\"java.util.ArrayList\");\n" +
//                "var Date = Java.type(\"java.util.Date\");\n" +
//                "var uuid = UUIDGenerator.uuid;\n" +
//                "var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n" +
//                "var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n" +
//                "var split_chinese = HanLPUtil.hanLPParticiple;\n" +
//                "var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n" +
//                "var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n" +
//                "var util = Java.type(\"com.tapdata.processor.util.Util\");\n" +
//                "var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n" +
//                "var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n" +
//                "var MD5 = function(str){return MD5Util.crypt(str, true);};\n" +
//                "var Collections = Java.type(\"java.util.Collections\");\n" +
//                "var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n" +
//                "var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n" +
//                "var TapModelDeclare = Java.type(\"com.tapdata.processor.util.TapModelDeclare\");\n" +
//                "var sleep = function(ms){\n" +
//                "var Thread = Java.type(\"java.lang.Thread\");\n" +
//                "Thread.sleep(ms);\n" +
//                "}\n";
}
