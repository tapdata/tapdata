package io.tapdata.js.connector.iengine;

import io.tapdata.entity.error.CoreException;
import io.tapdata.js.connector.enums.Constants;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;


public class LoadJavaScripter {

    private String jarFilePath ;
    private String flooder;
    private ScriptEngine scriptEngine;
    public ScriptEngine scriptEngine(){
        return this.scriptEngine;
    }

    //UnModify Map
    private Map<String,String> supportFunctions;
    public String supportFunctions(String functionName){
        if (Objects.isNull(this.supportFunctions)) return null;
        return this.supportFunctions.get(functionName);
    }
    public Map<String,String> supportFunctions(){
        return this.supportFunctions;
    }

    public LoadJavaScripter params(String jarFilePath,String flooder){
        this.jarFilePath = jarFilePath;
        this.flooder = flooder;
        return this;
    }

    public static LoadJavaScripter loader(String jarFilePath,String flooder){
        LoadJavaScripter loadJavaScripter = new LoadJavaScripter();
        return loadJavaScripter.params(jarFilePath,flooder).init();
    }
    public LoadJavaScripter init(){
        ScriptEngineManager engineManager = new ScriptEngineManager();
        this.scriptEngine = engineManager.getEngineByName("javascript");
//        this.scriptEngine = new ConnectorScriptEngine(null);
//        ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
//        this.scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().customEngine(ScriptFactory.TYPE_JAVASCRIPT, ConnectorScriptEngine.class));
        return this;
    }
    public ScriptEngine load(Enumeration<URL> resources){
        List<URL> list = new ArrayList<>();
        while (resources.hasMoreElements()){
            list.add(resources.nextElement());
        }
        try {
            for (URL url : list) {
                List<Map.Entry<InputStream,File>> files = javaScriptFiles(url);
                for (Map.Entry<InputStream,File> file : files) {
                    String path = file.getValue().getPath().replaceAll("\\\\","/");
                    //this.scriptEngine.eval(new FileReader(path));
                    this.scriptEngine.eval("load('" + path + "');");
                }
            }
            this.getSupportFunctions();
            return this.scriptEngine;
        }catch (Exception error){
            throw new CoreException("Error java script code, message: " + error.getMessage());
        }
    }

    //根据父路径加载全部JS文件并返回
    //connector.js必须放在最后
    //不存在connector.js就报错
    private List<Map.Entry<InputStream,File>> javaScriptFiles(URL url){
        Map.Entry<InputStream,File> connectorFile = null;
        List<Map.Entry<InputStream,File>> fileList = new ArrayList<>();
        String path = url.getPath();
        try {
            List<Map.Entry<InputStream,File>> collect = getAllFileFromJar(path);
            for (Map.Entry<InputStream,File> entry : collect) {
                File file = entry.getValue();
                if (this.fileIsConnectorJs(file)){
                    connectorFile = entry;
                    //this.getSupportFunctions(entry.getKey());
                }else {
                    fileList.add(entry);
                }
            }
        } catch (Exception ignored) {
            throw new CoreException(String.format("Unable to get the file list, the file directory is: %s. ",path));
        }
        if (Objects.isNull(connectorFile)){
            throw new CoreException("You must use connector.js as the entry of the data source. Please create a connector.js file and implement the data source method in this article.");
        }
        fileList.add(connectorFile);
        return fileList;
    }

    //获取connector.js内实现了的方法
    private void getSupportFunctions() {
        try {
            this.scriptEngine.eval("load('connectors-common/js-connector-core/src/main/java/io/tapdata/js/utils/evals.js');");
            Object functionGet = this.invoker("functionGet");
            Map<String,Object> functionList = (Map<String,Object>) fromJson(String.valueOf(functionGet));
            functionList.entrySet().stream().filter(Objects::nonNull).forEach(fun->{
                String funName = String.valueOf(fun.getValue());
                this.supportFunctions.put(funName,funName);
            });
            this.supportFunctions = Collections.unmodifiableMap(this.supportFunctions);
        }catch (Exception ignored){

        }
    }
    // var|let|const ... = function ...(){...}
    // (var|let|const)([ ]{1,n})(.*?)([ ]+)(=)([ ]+)(function[^\]{1,n}[ |\\n]+\\{[^\]+})

    // function xxx(){...}
    // (function[ ]{1,n}[^\]{1,n}[ ]+([^\]+)[ |\\n]+\\{[^\]+})

    // .*(TAP_TABLE\[[^\]]+).*

    private List<Map.Entry<InputStream,File>> getAllFileFromJar(String path){
        List<Map.Entry<InputStream,File>> fileList = new ArrayList<>();
        String pathJar = Objects.nonNull(jarFilePath) && !"".equals(jarFilePath) ?jarFilePath : path.replace("file:/","").replace("!/"+flooder,"");
        try {
            List<Map.Entry<ZipEntry, InputStream>> collect =
                    readJarFile(new JarFile(pathJar),flooder).collect(Collectors.toList());
            for (Map.Entry<ZipEntry, InputStream> entry : collect) {
                String key = entry.getKey().getName();
                InputStream stream = entry.getValue();
                fileList.add(new AbstractMap.SimpleEntry<InputStream,File>(stream,new File(key)));
            }
        } catch (IOException e) {
        }
        return fileList;
    }

    //@SneakyThrows
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
    private boolean fileIsConnectorJs(File file){
        return Objects.nonNull(file) && Constants.CONNECTOR_JS_NAME.equals(file.getName());
    }


    public Object invoker(String functionName,Object ... params){
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        if (Objects.isNull(this.scriptEngine)) return null;
        Invocable invocable = (Invocable) this.scriptEngine;
        try {
            Object invoke = invocable.invokeFunction(functionName, params);
            if (invoke instanceof Map || invoke instanceof Collection){
                return toJson(invoke);
            }
            return invoke;
        } catch (Exception e) {
            scriptException.set(e);
        }
        return null;
    }

//    public static void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
//        ScriptEngineManager engineManager = new ScriptEngineManager();
//        ScriptEngine scriptEngine = engineManager.getEngineByName("nashorn");
//        scriptEngine.eval("load('D:\\GavinData\\kitSpace\\tapdata\\plugin-kit\\tapdata-modules\\api-loader-module\\src\\main\\java\\io\\tapdata\\api\\apiJs\\connector.js');");
//        //scriptEngine.put("core", scriptCore);
//        //scriptEngine.put("log", new CustomLog());
//        AtomicReference<Throwable> scriptException = new AtomicReference<>();
//        Runnable runnable = () -> {
//            Invocable invocable = (Invocable) scriptEngine;
//            try {
//                invocable.invokeFunction("test");
//            } catch (Exception e) {
//                scriptException.set(e);
//            }
//        };
//        new Thread(runnable).start();
//    }
}
