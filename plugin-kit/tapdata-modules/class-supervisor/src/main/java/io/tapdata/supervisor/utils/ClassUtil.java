package io.tapdata.supervisor.utils;

import cn.hutool.json.JSONUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.*;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

public class ClassUtil {
    DependencyURLClassLoader dependencyURLClassLoader;
    List<URL> urls;

    public DependencyURLClassLoader getDependencyURLClassLoader() {
        return dependencyURLClassLoader;
    }

    public ClassUtil(String sourcePath) {
        urls = new ArrayList<>();
        Collection<File> jars = new ArrayList<>(FileUtils.listFiles(new File(sourcePath),
                FileFilterUtils.suffixFileFilter(".jar"),
                FileFilterUtils.directoryFileFilter()));
        jars.forEach(jar -> {
            String path = "jar:file://" + jar.getAbsolutePath() + "!/";
            try {
                urls.add(jar.toURI().toURL());
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        });

        dependencyURLClassLoader = new DependencyURLClassLoader(urls);
    }

    /**
     * @deprecated v1
     */
    //给一个接口，返回这个接口的所有实现类
    public static List<Class<?>> getAllClassByInterface(Class<?> c) {
        List<Class<?>> returnClassList = new ArrayList<Class<?>>(); //返回结果

        //如果不是一个接口，则不做处理
        if (c.isInterface()) {
            String packageName = c.getPackage().getName(); //获得当前的包名
            try {
                List<Class<?>> allClass = getClasses(packageName); //获得当前包下以及子包下的所有类

                //判断是否是同一个接口
                for (int i = 0; i < allClass.size(); i++) {
                    if (c.isAssignableFrom(allClass.get(i))) { //判断是不是一个接口
                        if (!c.equals(allClass.get(i))) { //本身不加进去
                            returnClassList.add(allClass.get(i));
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {

        }
        return returnClassList;
    }

    /**
     * @deprecated v1
     */
    //从一个包中查找出所有的类，在jar包中不能查找
    private static List<Class<?>> getClasses(String packageName)
            throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes;
    }

    /**
     * @deprecated v1
     */
    private static List<Class<?>> findClasses(File directory, String packageName)
            throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." +
                        file.getName()));
            } else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' +
                        file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }


    public Set<Class<?>> getClass(Class<?> superClass, String scanPackage) {
        if (Objects.nonNull(scanPackage) && "".equals(scanPackage.trim())) scanPackage = null;
        if (Objects.nonNull(this.dependencyURLClassLoader)) {
            ConfigurationBuilder builder = new ConfigurationBuilder()
                    .filterInputsBy(new FilterBuilder().include("^.*\\.class$"))
                    .setUrls(urls)
                    .addClassLoader(dependencyURLClassLoader.getActualClassLoader());
            if (Objects.nonNull(scanPackage)) {
                builder.forPackages(scanPackage);
            }
            Reflections reflections = new Reflections(builder);
            Set<Class<?>> aClass = (Optional.ofNullable(reflections.getSubTypesOf(superClass)).orElse(new HashSet<>())).stream()
                    .filter(c ->
                            Objects.nonNull(c) && !Modifier.isAbstract(c.getModifiers()) && !Modifier.isInterface(c.getModifiers())
                    ).collect(Collectors.toSet());
            if (!Modifier.isAbstract(superClass.getModifiers()) && !superClass.isInterface() && (null == scanPackage || superClass.getName().contains(scanPackage))) {
                aClass.add(superClass);
            }
            return aClass;
        }
        return null;
    }

    public Set<Class<?>> getClass(String superClass, String scanPackage) throws ClassNotFoundException {
        return getClass(Class.forName(superClass), scanPackage);
    }

    public static List<Map<String, Object>> jsonSource(String filePath, String sourcePath) {
        Collection<File> jars = new ArrayList<>(FileUtils.listFiles(
                new File(filePath),
                FileFilterUtils.suffixFileFilter(".jar"),
                FileFilterUtils.directoryFileFilter()));
        List<Map<String, Object>> fileFromJar = new ArrayList<>();
        for (File jar : jars) {
            try {
                URL url = jar.toURI().toURL();
                fileFromJar.addAll(getAllFileFromJar(url.getPath(), "", sourcePath));
            } catch (MalformedURLException e) {
                e.printStackTrace();
                // "MalformedURL {} while load jars, error {}", path, e.getMessage()
            }
        }
        return fileFromJar;
    }

    public static void main(String[] args) {
        jsonSource("D:\\GavinData\\kitSpace\\tapdata\\connectors\\dist\\activemq-connector-v1.0-SNAPSHOT.jar", "spec_activemq.json");
    }

    private static List<Map<String, Object>> getAllFileFromJar(String path, String flooder, String fileFlooder) {
        List<Map<String, Object>> fileList = new ArrayList<>();
        String pathJar = path.replace("file:/", "/").replace("!/" + flooder, "");
        try {
            List<Map.Entry<ZipEntry, InputStream>> collect =
                    readJarFile(new JarFile(pathJar), fileFlooder).collect(Collectors.toList());
            for (Map.Entry<ZipEntry, InputStream> entry : collect) {
                InputStream stream = entry.getValue();
                if (Objects.nonNull(stream)) {
                    fileList.add(JSONUtil.parseObj(fileToString(stream)));
                }
            }
        } catch (IOException e) {
        }
        return fileList;
    }

    private static Stream<Map.Entry<ZipEntry, InputStream>> readJarFile(JarFile jarFile, String prefix) {
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

    private static String fileToString(InputStream connectorJsStream) throws IOException {
        Reader reader = null;
        Writer writer = new StringWriter();
        char[] buffer = new char[1024];
        try {
            reader = new BufferedReader(new InputStreamReader(connectorJsStream, StandardCharsets.UTF_8));
            int n;
            while ((n = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, n);
            }
        } catch (Exception ignored) {
        } finally {
            if (Objects.nonNull(reader)) {
                reader.close();
                writer.close();
                connectorJsStream.close();
            }
        }
        return writer.toString();
    }
}
