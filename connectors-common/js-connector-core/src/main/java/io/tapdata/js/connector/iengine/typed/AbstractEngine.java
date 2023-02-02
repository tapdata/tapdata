package io.tapdata.js.connector.iengine.typed;

import io.tapdata.entity.error.CoreException;
import io.tapdata.js.connector.enums.Constants;

import javax.script.ScriptEngine;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

public abstract class AbstractEngine<T> implements EngineHandel {
    protected ScriptEngine scriptEngine;

    public static <T> T create() {
        return null;
    }

    protected abstract Map.Entry<String, EngineHandel> load(URL url);

    @Override
    public Map<String, EngineHandel> load(String jarFilePath, String flooder, Enumeration<URL> resources) {
        this.jarFilePath = jarFilePath;
        this.flooder = flooder;
        List<URL> list = new ArrayList<>();
        while (resources.hasMoreElements()) {
            list.add(resources.nextElement());
        }
        Map<String, EngineHandel> result = new HashMap<>();
        if (!list.isEmpty()) {
            list.stream().filter(Objects::nonNull)
                    .forEach(url ->
                            Optional.ofNullable(this.load(url))
                                    .ifPresent(consumer -> result.put(consumer.getKey(), consumer.getValue()))
                    );
        }
        return result;
    }


    protected String jarFilePath;
    protected String flooder;

    protected List<Map.Entry<InputStream, File>> getAllFileFromJar(String path) {
        List<Map.Entry<InputStream, File>> fileList = new ArrayList<>();
        String pathJar = Objects.nonNull(jarFilePath) && !"".equals(jarFilePath) ? jarFilePath : path.replace("file:/", "/").replace("!/" + flooder, "");
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

    //根据父路径加载全部JS文件并返回
    //connector.js必须放在最后
    //不存在connector.js就报错
    protected List<Map.Entry<InputStream, File>> javaScriptFiles(URL url) {
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

    //判断文件是否connector.js
    protected boolean fileIsConnectorJs(File file) {
        return Objects.nonNull(file) && Constants.CONNECTOR_JS_NAME.equals(file.getName());
    }

    protected Stream<Map.Entry<ZipEntry, InputStream>> readJarFile(JarFile jarFile, String prefix) {
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
}
