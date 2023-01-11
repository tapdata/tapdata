package io.tapdata.js.connector.iengine;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.js.connector.enums.Constants;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
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


public class LoadJavaScripter {
    private static final String TAG = LoadJavaScripter.class.getSimpleName();
    private static final String EVAL_ID = ScriptEngine.FILENAME;

    public static final String NASHORN_ENGINE = "nashorn";
    public static final String GRAAL_ENGINE = "graal.js";

    private String jarFilePath;
    private String flooder;
    private ScriptEngine scriptEngine;

    public ScriptEngine scriptEngine() {
        return this.scriptEngine;
    }

    private String ENGINE_TYPE = GRAAL_ENGINE;

    public static final String eval = "" +
            "var tapAPI = Java.type(\"io.tapdata.js.connector.server.decorator.APIFactoryDecorator\");";
    //UnModify Map
    /**
     * @deprecated
     * */
//    private Map<String,String> supportFunctions;
    /**
     * @deprecated
     * */
//    public String supportFunctions(String functionName){
//        if (Objects.isNull(this.supportFunctions)) return null;
//        return this.supportFunctions.get(functionName);
//    }

    /**
     * @deprecated
     */
//    public Map<String,String> supportFunctions(){
//        return this.supportFunctions;
//    }
    public LoadJavaScripter params(String jarFilePath, String flooder) {
        this.jarFilePath = jarFilePath;
        this.flooder = flooder;
        return this;
    }

    public static LoadJavaScripter loader(String jarFilePath, String flooder) {
        LoadJavaScripter loadJavaScripter = new LoadJavaScripter();
        return loadJavaScripter.params(jarFilePath, flooder).init();
    }

    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);

    public LoadJavaScripter init() {
//        ScriptEngineManager engineManager = new ScriptEngineManager();
//        this.scriptEngine = engineManager.getEngineByName(ENGINE_TYPE);//
        this.scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(ENGINE_TYPE));
        //GraalJSEngineFactory graalJSEngineFactory = new GraalJSEngineFactory();
        //scriptEngine = graalJSEngineFactory.getScriptEngine();
//        this.scriptEngine = new ConnectorScriptEngine(null);
//        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
//        this.scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().customEngine(ScriptFactory.TYPE_JAVASCRIPT, ConnectorScriptEngine.class));
        return this;
    }

    public ScriptEngine load(Enumeration<URL> resources) {
        List<URL> list = new ArrayList<>();
        while (resources.hasMoreElements()) {
            list.add(resources.nextElement());
        }
        try {
//            ScriptEngineManager engineManager = new ScriptEngineManager();
//            ScriptEngine itemScriptEngine = engineManager.getEngineByName("nashorn");
            //this.scriptEngine.getContext().setAttribute("js-nashorn-compat", "true", ScriptContext.GLOBAL_SCOPE);
            //this.scriptEngine.eval("load('"+EngineEvalResources.SCANNING_CAPABILITIES_RESOURCE+"');");
            for (URL url : list) {
                List<Map.Entry<InputStream, File>> files = javaScriptFiles(url);
                for (Map.Entry<InputStream, File> file : files) {
                    String path = file.getValue().getPath().replaceAll("\\\\", "/");
//                    this.scriptEngine.eval(new FileReader(path));
                    //this.scriptEngine.eval("onload('" + path + "');");
                    this.scriptEngine.eval("load('" + path + "');");
                    //scriptEngine.eval("load('"+path+"');");

//                    SimpleBindings simpleBindings = new SimpleBindings();
//                    scriptEngine.getBindings(ScriptContext.GLOBAL_SCOPE).put(ScriptEngine.FILENAME, path);
//                    simpleBindings.putAll(scriptEngine.getBindings(ScriptContext.GLOBAL_SCOPE));
//                    scriptEngine.eval(new InputStreamReader(file.getKey()), simpleBindings);

                    //this.scriptEngine.getContext().setAttribute(ScriptEngine.FILENAME, path, ScriptContext.GLOBAL_SCOPE);
                    //this.scriptEngine.eval(ScriptUtil.fileToString(file.getKey()),this.scriptEngine().getContext());
                    //Context polyglotContext = ((GraalJSScriptEngine) this.scriptEngine).getPolyglotContext();

//                    Engine engine = Engine.newBuilder().option("js.load-from-url","true").allowExperimentalOptions(true).build();
//                    Context context = Context.newBuilder().allowAllAccess(true).allowHostClassLoading(true).allowIO(true).engine(engine).build();;
                    //Value js = polyglotContext.eval(Source.create("js", "load('"+path+"');"));
//                    Source mysource = Source.newBuilder("js","load('"+path+"')","rongdemo").build();
//                  context.eval(mysource);

//                    this.scriptEngine.eval(mysource.getReader());
//                    Value callapp = context.getBindings("js").getMember("testString");
//                    System.out.println(callapp.execute());
                    //scriptEngine.eval("import * from '/resource:"+path+"';");
//                    scriptEngine.eval(new InputStreamReader(file.getKey()));
                }
            }
            //ScriptContext context = itemScriptEngine.getContext();
            //this.scriptEngine.eval(ScriptUtil.fileToString(file.getKey()));
//            try {
//                this.scriptEngine.eval(EngineEvalResources.js);
//            } catch (ScriptException ignored) {
//
//            }
//            this.getSupportFunctions();
            return this.scriptEngine;
        } catch (Exception error) {
            throw new CoreException("Error java script code, message: " + error.getMessage());
        }
    }

    //根据父路径加载全部JS文件并返回
    //connector.js必须放在最后
    //不存在connector.js就报错
    private List<Map.Entry<InputStream, File>> javaScriptFiles(URL url) {
        Map.Entry<InputStream, File> connectorFile = null;
        List<Map.Entry<InputStream, File>> fileList = new ArrayList<>();
        String path = url.getPath();
        try {
            List<Map.Entry<InputStream, File>> collect = getAllFileFromJar(path);
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

    /**
     * @deprecated 获取connector.js内实现了的方法
     **/
//    private void getSupportFunctions() {
//        try {
//            GraalJSScriptEngine context = (GraalJSScriptEngine)this.scriptEngine.getFactory().getScriptEngine();
//            Context polyglotContext = context.getPolyglotContext();
//            Value js = polyglotContext.getBindings("js");
//            Value tapFunctions = js.getMember("tapFunctions");
//            Map as = tapFunctions.as(Map.class);
//
//
//            ((Map)((Function)this.scriptEngine.get("discover_schema")).apply(null)).size()
//
//ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("write_record")).apply(null).toString())
//((Function)this.scriptEngine.get("testArrMap")).apply(null);
//((Function)this.scriptEngine.get("testArrMap")).apply(null).toString().replaceFirst("\\(2\\)","")
//ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("testArrMap")).apply(null).toString().replaceFirst("\\(([0-9]+)\\)",""))
//
//            ConnectorBase.fromJsonArray("(1)[{id: 2, name: 'kit'}, {id: 1}]".replaceFirst("\\(([0-9]+)\\)",""))
//            Object functionGet = this.invoker(JSFunctionNames.SCANNING_CAPABILITIES_IN_JAVA_SCRIPT.jsName());
//            Object funArr = this.scriptEngine.get("tapFunctions");
//            Map<String,Object> functionList = (Map<String,Object>) fromJson(String.valueOf(funArr));
//
//            functionList.stream().filter(Objects::nonNull).forEach(fun->{
//                String funName = String.valueOf(fun);
//                this.supportFunctions.put(funName,funName);
//            });
//
//            this.supportFunctions = Collections.unmodifiableMap(this.supportFunctions);
//        }catch (Exception ignored){
//
//        }
//    }
    private List<Map.Entry<InputStream, File>> getAllFileFromJar(String path) {
        List<Map.Entry<InputStream, File>> fileList = new ArrayList<>();
        String pathJar = Objects.nonNull(jarFilePath) && !"".equals(jarFilePath) ? jarFilePath : path.replace("file:/", "").replace("!/" + flooder, "");
        try {
            List<Map.Entry<ZipEntry, InputStream>> collect =
                    readJarFile(new JarFile(pathJar), flooder).collect(Collectors.toList());
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

    public Object arg(String argName) {
        return null;
    }

    public boolean functioned(String functionName) {
        if (Objects.isNull(functionName)) return false;
        if (Objects.isNull(this.scriptEngine)) return false;
        Object functionObj = this.scriptEngine.get(functionName);
        return functionObj instanceof Function;
    }

    private void binding(String key, Object name, int scope) {
        Bindings bindings = this.scriptEngine.getBindings(scope);
        bindings.put(key, name);
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
            //((Map)((Function)this.scriptEngine.get("discover_schema")).apply(null)).size()
            //ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("write_record")).apply(null).toString())
            //((Function)this.scriptEngine.get("testArrMap")).apply(null);
            //((Function)this.scriptEngine.get("testArrMap")).apply(null).toString().replaceFirst("\\(2\\)","")
            //ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("testArrMap")).apply(null).toString().replaceFirst("\\(([0-9]+)\\)",""))
            //ConnectorBase.fromJsonArray("(1)[{id: 2, name: 'kit'}, {id: 1}]".replaceFirst("\\(([0-9]+)\\)",""))


            //PolyglotMapAndFunction polyglotMapAndFunction = (PolyglotMapAndFunction)this.scriptEngine.get("batch_read");
            //PolyglotMap ployglotMap = (PolyglotMap) polyglotMapAndFunction.apply(null);
            //ployglotMap.get("0")e iterator = context1.getBindings(ScriptEngine.FILENAME).getIterator();
            //Object invoke = invocable.invokeFunction(functionName, params);

            //ARRAY
            //ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("discover_schema")).apply(null).toString())

            //Map
        } catch (Exception e) {
            //scriptException.set(e);
            TapLogger.warn(TAG, "Not function named " + functionName + " can be found.");
            return null;
        }
    }

    public Object invokerGraal(String functionName, Object... params) {
        if (Objects.isNull(functionName)) return null;
        if (Objects.isNull(this.scriptEngine)) return null;
        Function<Object[], Object> polyglotMapAndFunction;
        try {
            Invocable invocable = (Invocable) this.scriptEngine;
            Object apply = invocable.invokeFunction(functionName, params);
            //polyglotMapAndFunction = (Function<Object[], Object>) this.scriptEngine.get(functionName);
            //Object apply = polyglotMapAndFunction.apply(params);
            return LoadJavaScripter.covertData(apply);
            //((Map)((Function)this.scriptEngine.get("discover_schema")).apply(null)).size()
            //ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("write_record")).apply(null).toString())
            //((Function)this.scriptEngine.get("testArrMap")).apply(null);
            //((Function)this.scriptEngine.get("testArrMap")).apply(null).toString().replaceFirst("\\(2\\)","")
            //ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("testArrMap")).apply(null).toString().replaceFirst("\\(([0-9]+)\\)",""))
            //ConnectorBase.fromJsonArray("(1)[{id: 2, name: 'kit'}, {id: 1}]".replaceFirst("\\(([0-9]+)\\)",""))
            //PolyglotMapAndFunction polyglotMapAndFunction = (PolyglotMapAndFunction)this.scriptEngine.get("batch_read");
            //PolyglotMap ployglotMap = (PolyglotMap) polyglotMapAndFunction.apply(null);
            //ployglotMap.get("0")e iterator = context1.getBindings(ScriptEngine.FILENAME).getIterator();
            //Object invoke = invocable.invokeFunction(functionName, params);
            //ARRAY
            //ConnectorBase.fromJsonArray(((Function)this.scriptEngine.get("discover_schema")).apply(null).toString())
            //Map
        } catch (Exception e) {
            TapLogger.warn(TAG, "Not function named " + functionName + " can be found.");
            return null;
        }
    }


    public static Object covertData(Object apply){
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
                    //TapLogger.warn(TAG, "function named " + functionName + " exec failed, function return value is: " + apply.toString() + "error cast java Object.");
                    return null;
                }
            }
        } else {
            return apply;
        }
    }
}
