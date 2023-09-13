package io.tapdata.script.factory.py;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

public class PythonUtils {
    public static final String DEFAULT_PY_SCRIPT_START = "import json, random, time, datetime, uuid, types, yaml\n" +
            "import urllib, urllib2, requests\n" +
            "import math, hashlib, base64\n" +
            "def process(record, context):\n";
    public static final String DEFAULT_PY_SCRIPT = DEFAULT_PY_SCRIPT_START + "\treturn record;\n";

    public static final String PYTHON_THREAD_PACKAGE_PATH = "py-lib";
    public static final String PYTHON_THREAD_SITE_PACKAGES_PATH = "site-packages";
    public static final String PYTHON_THREAD_JAR = "jython-standalone-2.7.3.jar";

    public static File getThreadPackagePath(){
        File file = new File("py-lib/Lib/site-packages");
        if (!file.exists() || null == file.list() || file.list().length <= 0) {
            return null;
        }
        return file;
    }

    public static final String TAG = TapPythonEngine.class.getSimpleName();
    public static void flow(Log logger) {
        try {
            execute(PythonUtils.PYTHON_THREAD_JAR, PythonUtils.PYTHON_THREAD_PACKAGE_PATH, logger);
            if (!unzipIeJar(logger)){
                execute(PythonUtils.PYTHON_THREAD_JAR, PythonUtils.PYTHON_THREAD_PACKAGE_PATH, logger);
                return;
            }
            unzipPythonStandalone(logger);
            setPackagesResources(logger,
                    "py-lib/jython-standalone-2.7.3.jar",
                    PythonUtils.PYTHON_THREAD_PACKAGE_PATH,
                    "py-lib/agent/BOOT-INF/lib/jython-standalone-2.7.3.jar",
                    "jython-standalone-2.7.3.jar");
        } finally {
            File file = new File("py-lib/agent");
            if (file.exists()) {
                file.delete();
            }
        }
    }

    private static boolean unzipIeJar(Log logger) {
        final String tapdataAgentPath = "components/tapdata-agent.jar";
        final String unzipTapdataAgentPath = "py-lib/agent";
        if (new File(tapdataAgentPath).exists()) {
            ZipUtils.unzip(tapdataAgentPath, unzipTapdataAgentPath);
            return true;
        }
        return false;
    }

    private static void unzipPythonStandalone(Log logger) {
        final String pythonStandalone = "py-lib/agent/BOOT-INF/lib/jython-standalone-2.7.3.jar";
        try {
            copyFile(new File(pythonStandalone), new File(PythonUtils.PYTHON_THREAD_PACKAGE_PATH));
        } catch (Exception e) {
            logger.warn("Can not copy py-lib/agent/BOOT-INF/lib/jython-standalone-2.7.3.jar to py-lib, msg: {}", e.getMessage());
        }
    }

    private static void loopPackagesList(Log logger, final String loopPath, final String pythonJarPath){
        File loopFile = new File(loopPath);

        File[] files = loopFile.listFiles();
        if (null == files || files.length <= 0) return;
        for (File file : files) {
            String name = file.getName();
            File afterUnzipFile = null;
            if (file.exists() && file.isDirectory()) {
                afterUnzipFile = file;
            } else if (name.endsWith(".zip")){
                ZipUtils.unzip(file.getAbsolutePath(), file.getParentFile().getAbsolutePath());
                String afterZipFileName = name.substring(0, name.lastIndexOf(".zip"));
                afterUnzipFile = new File(file.getParentFile().getAbsolutePath() + File.separator + afterZipFileName);
            } else if (name.endsWith(".tar.gz")){
                ZipUtils.unzip(file.getAbsolutePath(), file.getParentFile().getAbsolutePath());
                String afterZipFileName = name.substring(0, name.lastIndexOf(".tar.gz"));
                afterUnzipFile = new File(file.getParentFile().getAbsolutePath() + File.separator + afterZipFileName);
            } else if (name.endsWith(".gz")){
                ZipUtils.unzip(file.getAbsolutePath(), file.getParentFile().getAbsolutePath());
                String afterZipFileName = name.substring(0, name.lastIndexOf(".gz"));
                afterUnzipFile = new File(file.getParentFile().getAbsolutePath() + File.separator + afterZipFileName);
            } else if (name.endsWith(".jar")) {
                ZipUtils.unzip(file.getAbsolutePath(), file.getParentFile().getAbsolutePath());
                String afterZipFileName = name.substring(0, name.lastIndexOf(".jar"));
                afterUnzipFile = new File(file.getParentFile().getAbsolutePath() + File.separator + afterZipFileName);
            }
            File[] fs = null == afterUnzipFile ? null : afterUnzipFile.listFiles();
            if (null != fs && fs.length > 0) {
                for (File f : fs) {
                    if (f.exists() && f.isFile() && f.getName().contains("setup.py")) {
                        try {
                            logger.info("{}'s resource package is being generated, please wait.", afterUnzipFile.getName());
                            ProcessBuilder command = new ProcessBuilder().command(
                                    "/bin/sh",
                                    "-c",
                                    String.format("cd %s; ", f.getParentFile().getAbsolutePath()) +
                                            String.format("java -jar %s %s install", new File(pythonJarPath).getAbsolutePath(), "setup.py"));
                            Process start = command.start();
                            start.waitFor();
                            logger.info("{}'s resource package is being generated", afterUnzipFile.getName());
                        } catch (Exception e){}
                        break;
                    }
                }
            }
        }
    }

    private static InputStream getLibPath(String jarName, Log log, AtomicReference<String> ato) throws IOException {
        InputStream pyJarPath = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader instanceof URLClassLoader) {
            URL[] urls = ((URLClassLoader) classLoader).getURLs();
            for (URL url : urls) {
                String jarPath = url.getPath();
                if (jarPath.contains(jarName)) {
                    pyJarPath = url.openStream();
                    ato.set(jarPath);
                    break;
                }
            }
        }
        return pyJarPath;
    }

    public static Integer execute(String jarName, String unzipPath, Log log) {
        AtomicReference<String> pyJarPathAto = new AtomicReference<>();
        try(InputStream inputStream = getLibPath(jarName, log, pyJarPathAto)) {
            String pyJarPath = pyJarPathAto.get();
            if (null == inputStream || null == pyJarPath) {
                return -1;
            }
            System.out.println(jarName);
            System.out.println(pyJarPath);
            System.out.println(unzipPath);
            File f = new File("py-lib");
            if (!f.exists()) f.mkdirs();
            final String zipFileTempPath = "py-lib/jython-standalone-2.7.3.jar";
            saveTempZipFile(inputStream, zipFileTempPath);
            return setPackagesResources(log, zipFileTempPath, unzipPath, pyJarPath, jarName);
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return 0;
    }

    private static int setPackagesResources(Log log, final String zipFileTempPath, String unzipPath, String pyJarPath, String jarName) {
        File file = new File(zipFileTempPath);
        final String libPathName = "temp_engine";
        try {
            try {
                try {
                    System.out.println("[1]: Start to unzip " + zipFileTempPath + ">>>");
                    ZipUtils.unzip(file.getAbsolutePath(), libPathName);
                    System.out.println("[2]: Unzip " + zipFileTempPath + " succeed >>>");
                } catch (Exception e) {
                    //TapLogger.error(TAG, "Can not zip from " + jarPath + " to " + libPathName);
                    System.out.println("[2]: Unzip " + zipFileTempPath + " fail, " + e.getMessage());
                    return -3;
                }
                File libFiles = new File(concat(libPathName, "Lib", PYTHON_THREAD_SITE_PACKAGES_PATH));
                if (!libFiles.exists()) {
                    System.out.println("[3]: Can not fund Lib path: " + libPathName + ", create file now >>>");
                    libFiles.mkdirs();
                    System.out.println("[4]: create file " + zipFileTempPath + " succeed >>>");
                } else {
                    System.out.println("[3]: Lib path is exists: " + libPathName + " >>>");
                    System.out.println("[4]: Copy after>>>");
                }
                System.out.println("[5]: Start to copy site-packages from Lib: " + libFiles + " to :" + unzipPath + " >>>");
                FileUtils.copyToDirectory(libFiles, new File(unzipPath));
                System.out.println("[6]: Copy site-packages to " + unzipPath + ", successfully");
            } finally {
                File temp1 = new File(libPathName);
                if (temp1.exists()) {
                    System.out.println("[*]: Start to clean temp files in" + libPathName);
                    FileUtils.deleteQuietly(temp1);
                    System.out.println("[*]: Temp file cleaned successfully");
                }
            }

            loopPackagesList(log, "py-lib/site-packages", zipFileTempPath);
        } catch (Throwable throwable) {
            System.out.println("[x]: can not prepare Lib for site-packages, Please manually extract " + zipFileTempPath + " and place " + zipFileTempPath + "/Lib/ in the " + (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath) + " folder.");
            log.warn("Init python resources failed: {}", throwable.getMessage());
            return -1;
        }
        return 1;
    }

    private static String concat(String path, String ...paths){
        for (String s : paths) {
            path = FilenameUtils.concat(path, s);
        }
        return path;
    }

    public static void saveTempZipFile(InputStream inputStream, String savePath){
        try {
            RandomAccessFile file = new RandomAccessFile(new File(savePath), "rw");
            file.close();
            OutputStream outputStream = new FileOutputStream(savePath);
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            // 关闭流
            inputStream.close();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int copyTo(String fromPath, String toPath) {
        try {
            File to = new File(toPath);
            File sourceDir = new File(fromPath);
            File[] from = sourceDir.listFiles();
            if (null == from) return -1;
            if (!to.exists()) {
                to.mkdirs();
            }
            for (File file : from) {
                copyFile(file, new File(to, file.getName()));
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, "Can not get python packages resources when load python engine, msg: {}", e.getMessage());
            return  -1;
        }
        return 1;
    }

    private static void copyFile(File file, File target) throws Exception {
        if (null == file) return;
        File[] files = file.listFiles();
        if (!target.exists() || !target.isDirectory()) target.mkdirs();
        if (null == files || files.length <= 0) return;
        for (File f : files) {
            if (f.isDirectory()) {
                copyFile(f, new File(FilenameUtils.concat(target.getPath(), f.getName())));
            } else if (f.isFile()) {
                copy(f, new File(FilenameUtils.concat(target.getPath(), f.getName())));
            }
        }
    }

    private static void copy(File from, File to) throws Exception{
        try (FileInputStream fis = new FileInputStream(from.getAbsolutePath());
             FileOutputStream fos = new FileOutputStream(to.getAbsolutePath())) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

}
