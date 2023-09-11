package io.tapdata.script.factory.py;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.python.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.python.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class PythonUtils {
    public static final String DEFAULT_PY_SCRIPT_START = "import json, random, time, datetime, uuid, types, yaml\n" + //", yaml"
            "import urllib, urllib2, requests\n" + //", requests"
            "import math, hashlib, base64\n" + //# , yaml, requests\n" +
            "def process(record, context):\n";
    public static final String DEFAULT_PY_SCRIPT = DEFAULT_PY_SCRIPT_START + "\treturn record;\n";

    public static final String PYTHON_THREAD_PACKAGE_PATH = "py-lib";
    public static final String PYTHON_THREAD_SITE_PACKAGES_PATH = "site-packages";
    public static final String PYTHON_THREAD_JAR = "jython-standalone-2.7.2.jar";

    public static String getThreadPackagePath(){
        return concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_THREAD_SITE_PACKAGES_PATH);
    }

    public static final String TAG = TapPythonEngine.class.getSimpleName();

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
    /**
     * @deprecated
     * */
    public static Integer execute0(String jarName, String unzipPath, Log log) {
        String pyJarPath = "";//getLibPath(jarName, log);
        if (null == pyJarPath) {
            return -1;
        }
        System.out.println(jarName);
        System.out.println(pyJarPath);
        System.out.println(unzipPath);
        final String jarPath = pyJarPath.endsWith(jarName) ? pyJarPath : (pyJarPath + jarName);
        File file = new File(jarPath);
        if (!file.exists() || !file.isFile()){
            log.warn("Miss jar path: {}", jarPath);
            return -2;
        }
        final String libPathName = "temp_engine";
        try {
            try {
                try {
                    System.out.println("[1]: Start to unzip " + jarPath + ">>>");
                    unzip(file.getAbsolutePath(), libPathName);
                    System.out.println("[2]: Unzip " + jarPath + " succeed >>>");
                }catch (Exception e){
                    //TapLogger.error(TAG, "Can not zip from " + jarPath + " to " + libPathName);
                    System.out.println("[2]: Unzip " + jarPath + " fail.");
                    return -3;
                }
                File libFiles = new File(concat(libPathName, "Lib", PYTHON_THREAD_SITE_PACKAGES_PATH));
                if (!libFiles.exists()) {
                    System.out.println("[3]: Can not fund Lib path: " + libPathName + ", create file now >>>");
                    libFiles.createNewFile();
                    System.out.println("[4]: create file " + jarPath + " succeed >>>");
                } else {
                    System.out.println("[3]: Lib path is exists: " + libPathName + " >>>");
                    System.out.println("[4]: Copy after>>>");
                }
                System.out.println("[5]: Start to copy site-packages from Lib: " + libFiles + " to :" + unzipPath + " >>>");
                FileUtils.copyToDirectory(libFiles, new File(unzipPath));
                System.out.println("[6]: Copy site-packages to " + unzipPath + ", successfully" );
            } finally {
                File temp1 = new File(libPathName);
                if (temp1.exists()) {
                    System.out.println("[*]: Start to clean temp files in" + libPathName);
                    FileUtils.deleteQuietly(temp1);
                    System.out.println("[*]: Temp file cleaned successfully" );
                }
            }
        } catch (Throwable throwable) {
            System.out.println("[x]: can not prepare Lib for site-packages, Please manually extract " + jarPath + " and place " + jarPath  + "/Lib/ in the " + (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath ) + " folder." );
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "Class modify failed", throwable);
            return -1;
        }
        return 0;
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
            final String zipFileTempPath = "py-lib/lib.zip";
            saveTempZipFile(inputStream, zipFileTempPath);
            File file = new File(zipFileTempPath);
            final String libPathName = "temp_engine";
            try {
                try {
                    try {
                        System.out.println("[1]: Start to unzip " + zipFileTempPath + ">>>");
                        unzip(file.getAbsolutePath(), libPathName);
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
            } catch (Throwable throwable) {
                System.out.println("[x]: can not prepare Lib for site-packages, Please manually extract " + zipFileTempPath + " and place " + zipFileTempPath + "/Lib/ in the " + (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath) + " folder.");
                log.warn("Init python resources failed: {}", throwable.getMessage());
                return -1;
            }
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return 0;
    }

    private static String concat(String path, String ...paths){
        for (String s : paths) {
            path = FilenameUtils.concat(path, s);
        }
        return path;
    }

    public static void unzip(String zipFile, String outputPath) {
        if (zipFile == null || outputPath == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        File outputDir = new File(outputPath);
        if (outputDir.isFile())
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");
        if (zipFile.endsWith(".tar.gz") || zipFile.endsWith(".gz")){
            unTarZip(zipFile, outputPath);
        } else {
            unzip(zipFile, outputDir);
        }
    }

    public static void unTarZip(String tarFilePath, String targetDirectoryPath){
        try (InputStream inputStream = new FileInputStream(tarFilePath)) {
            unTarZip(inputStream, targetDirectoryPath);
        } catch (Exception e){
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none, " + e.getMessage());
        }
    }

    public static void unTarZip(InputStream inputStream, String targetDirectoryPath) throws Exception {
        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(inputStream);
        TarArchiveEntry entry;
        while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
            File outputFile = new File(targetDirectoryPath, entry.getName());
            if (entry.isDirectory()) {
                if (!outputFile.exists()) {
                    outputFile.mkdirs();
                }
                continue;
            }
            outputFile.getParentFile().mkdirs();
            try (OutputStream outputStream = new FileOutputStream(outputFile)) {
                byte[] buffer = new byte[4096];
                int len;
                while ((len = tarArchiveInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, len);
                }
            }
        }
        tarArchiveInputStream.close();
    }

    public static void unzip(String zipFile, File outputDir) {
        if (zipFile == null || outputDir == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        if (outputDir.isFile())
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");

        try (ZipFile zf = new ZipFile(zipFile)) {

            if (!outputDir.exists())
                FileUtils.forceMkdir(outputDir);

            Enumeration<? extends ZipEntry> zipEntries = zf.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry entry = zipEntries.nextElement();

                try {
                    if (entry.isDirectory()) {
                        String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
                        FileUtils.forceMkdir(new File(entryPath));
                    } else {
                        String entryPath = FilenameUtils.concat(outputDir.getAbsolutePath(), entry.getName());
                        try(OutputStream fos = FileUtils.openOutputStream(new File(entryPath))) {
                            IOUtils.copyLarge(zf.getInputStream(entry), fos);
                        }
                    }
                } catch (IOException ei) {
                    ei.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @deprecated
     * */
    public static void unzip(InputStream zipFile, File outputDir) {
        if (zipFile == null || outputDir == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Unzip missing zipFile or outputPath");
        if (outputDir.isFile())
            throw new CoreException(PDKRunnerErrorCodes.CLI_UNZIP_DIR_IS_FILE, "Unzip director is a file, expect to be directory or none");

        try (ZipInputStream zf = new ZipInputStream(zipFile)) {

            if (!outputDir.exists())
                FileUtils.forceMkdir(outputDir);

            byte[] buffer = new byte[1024];
            ZipEntry zipEntry = null;
            while (null != (zipEntry = zf.getNextEntry())) {
                String fileName = zipEntry.getName();
                File newFile = new File(outputDir.getAbsolutePath() + File.separator + fileName);
                // 如果当前条目是文件夹，则创建相应的文件夹
                if ("".equals(fileName) || zipEntry.isDirectory()) {
                    new File(newFile.getAbsolutePath()).mkdirs();
                } else {
                    // 如果当前条目是文件，则创建一个FileOutputStream对象
                    FileOutputStream fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zf.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                    fos.close();
                }
                zf.closeEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveTempZipFile(InputStream inputStream, String savePath){
        try {
//            File file = new File(savePath);
//            if (!file.exists()) file.createNewFile();
            RandomAccessFile file = new RandomAccessFile(new File(savePath), "rw");
            file.close();
            // 创建输出流
            OutputStream outputStream = new FileOutputStream(savePath);
            // 缓冲区大小，可根据实际情况调整
            byte[] buffer = new byte[1024];
            // 读取输入流中的数据，并写入输出流
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            // 关闭流
            inputStream.close();
            outputStream.close();
            System.out.println("文件写入成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
