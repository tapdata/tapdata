package com.tapdata.tm.commons.util;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/9 下午6:44
 */
public class Loader {

    public static boolean isExtends(Class<?> clazz, Class<?> supperClass) {
        if(clazz == null)
            return false;
        if (clazz.getSuperclass() == supperClass)
            return true;
        if (clazz.getSuperclass() == null)
            return false;
        return isExtends(clazz.getSuperclass(), supperClass);
    }

    public static List<String> getResourceFiles(String basePackageName) throws IOException {
        String packageSearchPath = basePackageName.replace(".", "/");
        ClassLoader cl = Loader.class.getClassLoader();
        Set<URL> paths = new LinkedHashSet<>(16);
        Enumeration<URL> resourceUrls = (cl != null ? cl.getResources(packageSearchPath) : ClassLoader.getSystemResources(packageSearchPath));
        while (resourceUrls.hasMoreElements()) {
            URL url = resourceUrls.nextElement();
            paths.add(url);
        }
        final String _packageSearchPath = packageSearchPath.replace("/", File.separator);
        return paths.stream().flatMap(url -> Loader.getResourceFiles(url).stream())
                .filter(filePath -> filePath.contains(packageSearchPath) ||
                            filePath.contains(_packageSearchPath))
                .map(filePath ->
                {
                    int index = filePath.contains(packageSearchPath) ? filePath.indexOf(packageSearchPath)
                            : filePath.indexOf(_packageSearchPath);
                    return filePath.substring(index, filePath.length() - 6)
                            .replaceAll("[/\\\\]", ".");
                })
                .collect(Collectors.toList());
    }

    public static List<String> getResourceFiles(URL url) {
        if (url == null) {
            return null;
        }
        if (url.getProtocol().startsWith("file")) {
            List<String> list = listFiles(new File(url.getPath()));
            return list;
        }
        if (url.getProtocol().startsWith("jar")) {

            try {
                JarFile jarFile = ((JarURLConnection) url.openConnection()).getJarFile();
                return listJarFiles(jarFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Collections.emptyList();
        }
        return Collections.emptyList();
    }

    public static List<String> listJarFiles(JarFile jarFile) {

        if (jarFile == null)
            return Collections.emptyList();
        // jar:file:/data/refactor-enterprise/tapdata-v1.25.2/components/tapdata-agent-20211221-2.jar!/BOOT-INF/lib/tm-common-0.0.1-SNAPSHOT.jar!/com/tapdata/tm/commons/dag

        return jarFile.stream().filter(jarEntry -> !jarEntry.isDirectory() && jarEntry.getName().endsWith(".class"))
                .map(ZipEntry::getName).collect(Collectors.toList());
    }

    public static List<String> listFiles(File file) {
        if (file == null)
            return Collections.emptyList();
        if (file.canRead() && file.isFile() && file.getName().endsWith("class")) {
            return Collections.singletonList(file.getAbsolutePath());
        } else if (file.canRead() && file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                return Arrays.stream(files).map(Loader::listFiles).flatMap(Collection::stream).collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }
}
