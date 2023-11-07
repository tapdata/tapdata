package io.tapdata.observable.metric.py;

import io.tapdata.entity.logger.Log;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PythonUtils {

    private PythonUtils() {

    }

    private static final String PACKAGE_COMPILATION_COMMAND = "cd %s; java -jar %s setup.py install";
    private static final String PACKAGE_COMPILATION_FILE = "setup.py";
    public static final String PYTHON_THREAD_PACKAGE_PATH = "py-lib";
    public static final String PYTHON_THREAD_SITE_PACKAGES_PATH = "site-packages";
    public static final String PYTHON_THREAD_JAR = "jython-standalone-2.7.3.jar";
    public static final String PYTHON_SITE_PACKAGES_VERSION_CONFIG = "install.json";
    private static final String AGENT_TAG = "agent";
    private static final String BOOT_INF_TAG = "BOOT-INF";
    private static final String LIB_TAG = "lib";
    private static final String LIB_U_TAG = "Lib";
    public static final String TAG = PythonUtils.class.getSimpleName();

    public static void flow(Log logger) {
        try {
            if (!unzipIeJar()){
                execute(PythonUtils.PYTHON_THREAD_JAR, PythonUtils.PYTHON_THREAD_PACKAGE_PATH, logger);
                return;
            }
            unzipPythonStandalone(logger);
            setPackagesResources(logger,
                    concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_THREAD_JAR),
                    PythonUtils.PYTHON_THREAD_PACKAGE_PATH,
                    concat(PYTHON_THREAD_PACKAGE_PATH, AGENT_TAG, BOOT_INF_TAG, LIB_TAG, PYTHON_THREAD_JAR),
                    PYTHON_THREAD_JAR);
        } finally {
            deleteFile(new File(concat(PYTHON_THREAD_PACKAGE_PATH, AGENT_TAG)), logger);
            deleteFile(new File(concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_THREAD_SITE_PACKAGES_PATH, PYTHON_THREAD_SITE_PACKAGES_PATH)), logger);
        }
    }

    public static void deleteFile(File file, Log logger) {
        if (file.exists()) {
            try {
                if (file.isDirectory()) {
                    FileUtils.deleteDirectory(file);
                } else {
                    FileUtils.delete(file);
                }
            } catch (Exception e){
                logger.info("file not be delete, file: {}, message: {}", file.getAbsolutePath(), e.getMessage());
            }
        }
    }

    private static boolean unzipIeJar() {
        final String tapAgentPath = "components/tapdata-agent.jar";
        final String unzipTapAgentPath = concat(PYTHON_THREAD_PACKAGE_PATH,AGENT_TAG);
        if (new File(tapAgentPath).exists()) {
            ZipUtils.unzip(tapAgentPath, unzipTapAgentPath);
            return true;
        }
        return false;
    }

    private static void unzipPythonStandalone(Log logger) {
        final String pythonStandalone = concat(PYTHON_THREAD_PACKAGE_PATH, AGENT_TAG, BOOT_INF_TAG, LIB_TAG, PYTHON_THREAD_JAR);
        try {
            copyFile(new File(pythonStandalone), new File(PythonUtils.PYTHON_THREAD_PACKAGE_PATH));
        } catch (Exception e) {
            logger.warn("Can not copy {} to {}, msg: {}", pythonStandalone, PYTHON_THREAD_PACKAGE_PATH, e.getMessage());
        }
    }

    private static void loopPackagesList(Log logger, final String loopPath, final String pythonJarPath){
        File config = new File(concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_SITE_PACKAGES_VERSION_CONFIG));
        if (config.exists() && config.isFile()) {
            //按照配置文件来编译第三方Python包
            Map<String, Object> configMap = getPythonConfig(config);
            Object sitePackages = configMap.get(PYTHON_THREAD_SITE_PACKAGES_PATH);
            if (sitePackages instanceof Collection) {
                Collection<?> packages = null;
                try {
                    packages = (Collection<?>) sitePackages;
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
                if (!packages.isEmpty()) {
                    logger.info("Configuration files will be used for package compilation： {}", toJson(configMap));
                    List<File> path = new ArrayList<>();
                    for (Object name : packages) {
                        path.add(new File(concat(loopPath, String.valueOf(name))));
                    }
                    loopFiles(path, logger, pythonJarPath);
                    return;
                }
            }
        }
        // 以默认方式遍历需要编译的第三方Python包
        File loopFile = new File(loopPath);
        File[] files = loopFile.listFiles();
        loopFiles(null == files ? null : new ArrayList<>(Arrays.asList(files)), logger, pythonJarPath);
    }

    private static void loopFiles(List<File> files, Log logger, final String pythonJarPath) {
        if (null == files || files.isEmpty()) return;
        for (File file : files) {
            File afterUnzipFile = FixFileUtil.fixFile(file);
            File[] fs = null;
            if (null != afterUnzipFile) {
                fs = afterUnzipFile.listFiles();
            }
            if (null == fs || fs.length <= 0) {
                continue;
            }
            for (File f : fs) {
                if (f.exists() && f.isFile() && f.getName().contains(PACKAGE_COMPILATION_FILE)) continue;
                unPackageFile(f, afterUnzipFile, pythonJarPath, logger);
            }
        }
    }

    private static void unPackageFile(File f, File afterUnzipFile, final String pythonJarPath, Log logger) {
        Process start = null;
        try {
            logger.info("{}'s resource package is being generated, please wait.", afterUnzipFile.getName());
            ProcessBuilder command = new ProcessBuilder().command(
                    "/bin/sh",
                    "-c",
                    String.format(PACKAGE_COMPILATION_COMMAND,
                            f.getParentFile().getAbsolutePath(),
                            new File(pythonJarPath).getAbsolutePath()));
            start = command.start();
            start.waitFor();
            logger.info("{}'s resource package is being generated", afterUnzipFile.getName());
        } catch (IOException e) {
            logger.warn("{}'s resource package is generated failed, msg: {},", e.getMessage());
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            Thread.currentThread().interrupt();
        } finally {
            Optional.ofNullable(start).ifPresent(Process::destroy);
        }

        StringJoiner errorMsg = new StringJoiner(System.lineSeparator());
        if (null != start && !start.isAlive()) {
            try (InputStream errorStream = start.getErrorStream();
                 BufferedReader br = new BufferedReader(new InputStreamReader(errorStream))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    errorMsg.add(line);
                }
            } catch (Exception msgException) {
                logger.warn("Can't show execute message when execute release packages process, message: {}, Compilation information: {}", msgException.getMessage(), errorMsg.toString());
            }
        }
    }

    private static InputStream getLibPath(String jarName, AtomicReference<String> ato) throws IOException {
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

    private static Integer execute(String jarName, String unzipPath, Log log) {
        AtomicReference<String> pyJarPathAto = new AtomicReference<>();
        try(InputStream inputStream = getLibPath(jarName, pyJarPathAto)) {
            String pyJarPath = pyJarPathAto.get();
            if (null == inputStream || null == pyJarPath) {
                return -1;
            }
            File f = new File(PYTHON_THREAD_PACKAGE_PATH);
            if (!f.exists()) f.mkdirs();
            final String zipFileTempPath = concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_THREAD_JAR);
            saveTempZipFile(inputStream, zipFileTempPath, log);
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
            log.info("[1]: Start to unzip {}>>>", zipFileTempPath);
            ZipUtils.unzip(file.getAbsolutePath(), libPathName);
            log.info("[2]: Unzip {} succeed >>>", zipFileTempPath);
        } catch (Exception e) {
            log.info("[2]: Unzip {} fail, {}", zipFileTempPath, e.getMessage());
            return -3;
        }

        try {
            copyFile(new File("temp_engine/jython.jar"), new File(PythonUtils.PYTHON_THREAD_PACKAGE_PATH));
            File libFiles = new File(concat(libPathName, LIB_U_TAG, PYTHON_THREAD_SITE_PACKAGES_PATH));
            if (!libFiles.exists()) {
                log.info("[3]: Can not fund Lib path: {}, create file now >>>", libPathName);
                boolean mkDirs = libFiles.mkdirs();
                log.info("[4]: create file {} succeed: {} >>>", zipFileTempPath, mkDirs);
            } else {
                log.info("[3]: Lib path is exists: {} >>>[4]: Copy after>>>", libPathName);
            }
            log.info("[5]: Start to copy {} from Lib: {} to : {} >>>", PYTHON_THREAD_SITE_PACKAGES_PATH, libFiles, unzipPath);
            FileUtils.copyToDirectory(libFiles, new File(unzipPath));

            File jarLib = new File(concat(libPathName, LIB_U_TAG));
            File toPath = new File(concat(PYTHON_THREAD_PACKAGE_PATH, LIB_U_TAG, PYTHON_THREAD_SITE_PACKAGES_PATH));
            if (!toPath.exists() && toPath.mkdirs()) {
                copyFile(jarLib, toPath);
                copyFile(new File(concat(libPathName, PYTHON_SITE_PACKAGES_VERSION_CONFIG)), new File(PYTHON_THREAD_PACKAGE_PATH));
                File needDelete = new File(concat(PYTHON_THREAD_PACKAGE_PATH, LIB_U_TAG, "ite-packages", PYTHON_THREAD_SITE_PACKAGES_PATH));
                if (needDelete.exists()) {
                    FileUtils.deleteDirectory(needDelete);
                }
                log.info("[6]: Copy {} to {}, successfully", PYTHON_THREAD_SITE_PACKAGES_PATH, unzipPath);
            }
        }catch (Exception e) {
            log.info("[x]: can not prepare Lib for {}, Please manually extract {} and place {}/Lib/ in the {} folder",
                    PYTHON_THREAD_SITE_PACKAGES_PATH,
                    zipFileTempPath,
                    zipFileTempPath,
                    (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath));
            log.warn("Init python resources failed: {}", e.getMessage());
            return -1;
        } finally {
            File temp1 = new File(libPathName);
            if (temp1.exists()) {
                log.info("[*]: Start to clean temp files in {}", libPathName);
                FileUtils.deleteQuietly(temp1);
                log.info("[*]: Temp file cleaned successfully");
            }
        }
        loopPackagesList(log, concat(PYTHON_THREAD_PACKAGE_PATH,PYTHON_THREAD_SITE_PACKAGES_PATH), zipFileTempPath);
        return 1;
    }

    public static String concat(String path, String ...paths){
        for (String s : paths) {
            path = FilenameUtils.concat(path, s);
        }
        return path;
    }

    private static void saveTempZipFile(InputStream inputStream, String savePath, Log log){
        try (OutputStream outputStream = new FileOutputStream(savePath)) {
            RandomAccessFile file = new RandomAccessFile(new File(savePath), "rw");
            file.close();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            log.info(e.getMessage());
        } finally {
            try {
                if (null != inputStream) {
                    inputStream.close();
                }
            } catch (IOException e) {
                log.info(e.getMessage());
            }
        }
    }

    public static void copyFile(File file, File target) throws Exception {
        if (null == file) return;
        File[] files = null;
        if (file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }
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

    private static void copy(File from, File to) throws IOException {
        try (FileInputStream fis = new FileInputStream(from.getAbsolutePath());
             FileOutputStream fos = new FileOutputStream(to.getAbsolutePath())) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

    public static Map<String, Object> getPythonConfig(File configFile){
        if (null == configFile || !configFile.exists() || !configFile.isFile()) return new HashMap<>();
        try {
            return (Map<String, Object>) fromJson(FileUtils.readFileToString(configFile, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return new HashMap<>();
        }
    }
}
