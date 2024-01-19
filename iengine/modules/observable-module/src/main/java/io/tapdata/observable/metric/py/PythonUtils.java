package io.tapdata.observable.metric.py;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PythonUtils {

    protected static final String PACKAGE_COMPILATION_COMMAND = "cd %s; java -jar %s setup.py install";
    protected static final String PACKAGE_COMPILATION_FILE = "setup.py";
    public static final String PYTHON_THREAD_PACKAGE_PATH = "py-lib";
    public static final String PYTHON_THREAD_SITE_PACKAGES_PATH = "site-packages";
    public static final String PYTHON_THREAD_JAR = "jython-standalone-2.7.3.jar";
    public static final String PYTHON_SITE_PACKAGES_VERSION_CONFIG = "install.json";
    protected static final String AGENT_TAG = "agent";
    protected static final String BOOT_INF_TAG = "BOOT-INF";
    protected static final String LIB_TAG = "lib";
    protected static final String LIB_U_TAG = "Lib";
    public static final String TAG = PythonUtils.class.getSimpleName();

    public static void flow(TapLog tapLog) {
        if (null == tapLog) throw new IllegalArgumentException("Tap log can not be empty");
        PythonUtils utils = create();
        utils.flowStart(tapLog);
    }

    protected static PythonUtils create() {
        return new PythonUtils();
    }

    public void flowStart(Log logger){
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

    protected void deleteFile(File file, Log logger) {
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

    protected boolean unzipIeJar(){
        final String tapAgentPath = "components/tapdata-agent.jar";
        final String unzipTapAgentPath = concat(PYTHON_THREAD_PACKAGE_PATH, AGENT_TAG);
        return unzipIeJar(new File(tapAgentPath), unzipTapAgentPath);
    }

    protected boolean unzipIeJar(File tapAgentFile, String unzipTapAgentPath) {
        if (null != tapAgentFile && tapAgentFile.exists()) {
            try {
                ZipUtils.unzip(tapAgentFile.getAbsolutePath(), unzipTapAgentPath);
            } catch (Exception e) {
                return false;
            }
            return true;
        }
        return false;
    }

    protected void unzipPythonStandalone(Log logger) {
        final String pythonStandalone = concat(PYTHON_THREAD_PACKAGE_PATH, AGENT_TAG, BOOT_INF_TAG, LIB_TAG, PYTHON_THREAD_JAR);
        try {
            copyFile(new File(pythonStandalone), new File(PythonUtils.PYTHON_THREAD_PACKAGE_PATH));
        } catch (Exception e) {
            logger.warn("Can not copy {} to {}, msg: {}", pythonStandalone, PYTHON_THREAD_PACKAGE_PATH, e.getMessage());
        }
    }

    protected File getLoopPackagesFile() {
        String path = concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_SITE_PACKAGES_VERSION_CONFIG);
        return new File(path);
    }

    protected boolean loopByConfig(File config, Log logger, final String loopPath, final String pythonJarPath) {
        //按照配置文件来编译第三方Python包
        Map<String, Object> configMap = getPythonConfig(config);
        Object sitePackages = configMap.get(PYTHON_THREAD_SITE_PACKAGES_PATH);
        if (sitePackages instanceof Collection) {
            Collection<String> packages = (Collection<String>) sitePackages;
            if (!packages.isEmpty()) {
                logger.info("Configuration files will be used for package compilation： {}", toJson(configMap));
                List<File> path = new ArrayList<>();
                for (Object name : packages) {
                    if (null == name) continue;
                    path.add(new File(concat(loopPath, String.valueOf(name))));
                }
                if (!path.isEmpty()) {
                    loopFiles(path, logger, pythonJarPath);
                    return true;
                }
            }
        }
        return false;
    }

    protected void loopPackagesList(File config, Log logger, final String loopPath, final String pythonJarPath) {
        if (null == config) return;
        if (config.exists() && config.isFile()) {
            boolean byConfig = loopByConfig(config, logger, loopPath, pythonJarPath);
            if (byConfig) return;
        }
        // 以默认方式遍历需要编译的第三方Python包
        File loopFile = new File(loopPath);
        doLoopStart(loopFile, pythonJarPath, logger);
    }

    protected void doLoopStart(File loopFile, String pythonJarPath, Log logger) {
        File[] files = loopFile.listFiles();
        if (null == files) return;
        List<File> list = new ArrayList<>(Arrays.asList(files));
        loopFiles(list, logger, pythonJarPath);
    }

    protected void loopFiles(List<File> files, Log logger, final String pythonJarPath) {
        if (null != files) {
            for (File file : files) {
                if (null == file) continue;
                File afterUnzipFile = FixFileUtil.fixFile(file);
                File setUpPyFile = getSetUpPyFile(afterUnzipFile);
                unPackageBySetUpPy(setUpPyFile, afterUnzipFile, pythonJarPath, logger);
            }
        }
    }

    protected File getSetUpPyFile(File parentFile) {
        String path = concat(parentFile.getAbsolutePath(), PACKAGE_COMPILATION_FILE);
        return new File(path);
    }

    protected void unPackageBySetUpPy(File setUpPyFile, File afterUnzipFile, String pythonJarPath, Log logger) {
        if (null != setUpPyFile && setUpPyFile.exists() && setUpPyFile.isFile()) {
            unPackageFile(setUpPyFile, afterUnzipFile, pythonJarPath, logger);
        }
    }

    protected void unPackageFile(File setUpPyFile, File afterUnzipFile, final String pythonJarPath, Log logger) {
        Process start = null;
        try {
            String tag = afterUnzipFile.getName();
            logger.info("{}'s resource package is being generating, please wait", tag);
            ProcessBuilder command = getUnPackageFileProcessBuilder(setUpPyFile.getParentFile().getAbsolutePath(), pythonJarPath);
            start = command.start();
            printInfo(start.getInputStream(), logger, tag);
            printInfo(start.getErrorStream(), logger, tag);
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
    }

    protected void printInfo(InputStream stream, Log logger, String tag) {
        new Thread(() -> printMsg(stream, logger), "PYTHON-NODE-INSTALLER-" + UUID.randomUUID().toString() + "-" + tag).start();
    }

    protected void printMsg(InputStream stream, Log log) {
        if (null == stream) return;
        try (BufferedReader reader = getBufferedReader(stream)) {
            printMsg(reader, log);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }
    protected BufferedReader getBufferedReader(InputStream stream) {
        return new BufferedReader(new InputStreamReader(stream));
    }

    protected void printMsg(BufferedReader reader, Log log) {
        if (null == reader) return;
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info(line);
            }
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    protected ProcessBuilder getUnPackageFileProcessBuilder(String unPackageAbsolutePath, String pythonJarPath) {
        if (null == pythonJarPath) {
            throw new CoreException("Can not find python jar by an empty path name");
        }
        return new ProcessBuilder().command("/bin/sh", "-c",
                String.format(PACKAGE_COMPILATION_COMMAND, unPackageAbsolutePath, new File(pythonJarPath).getAbsolutePath()));
    }

    protected InputStream getLibPath(String jarName, AtomicReference<String> ato) throws IOException {
        InputStream pyJarPath = null;
        ClassLoader classLoader = getCurrentThreadContextClassLoader();
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

    protected ClassLoader getCurrentThreadContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    protected File getPythonThreadPackageFile(){
        return new File(PYTHON_THREAD_PACKAGE_PATH);
    }

    protected AtomicReference<String> getAtomicReference() {
        return new AtomicReference<>();
    }

    protected Integer execute(String jarName, String unzipPath, Log log) {
        AtomicReference<String> pyJarPathAto = getAtomicReference();
        try(InputStream inputStream = getLibPath(jarName, pyJarPathAto)) {
            String pyJarPath = pyJarPathAto.get();
            if (null == inputStream || null == pyJarPath) {
                return -1;
            }
            File f = getPythonThreadPackageFile();
            if (!f.exists()) f.mkdirs();
            final String zipFileTempPath = concat(PYTHON_THREAD_PACKAGE_PATH, PYTHON_THREAD_JAR);
            saveTempZipFile(inputStream, zipFileTempPath, log);
            return setPackagesResources(log, zipFileTempPath, unzipPath, pyJarPath, jarName);
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return 0;
    }

    protected boolean unZipAsCacheFile(File file, final String zipFileTempPath, final String libPathName, Log log) {
        try {
            log.info("[1]: Start to unzip {}>>>", zipFileTempPath);
            ZipUtils.unzip(file.getAbsolutePath(), libPathName);
            log.info("[2]: Unzip {} succeed >>>", zipFileTempPath);
        } catch (Exception e) {
            log.info("[2]: Unzip {} fail, {}", zipFileTempPath, e.getMessage());
            return false;
        }
        return true;
    }

    protected File beforeCopyLibFiles(String libPathName) throws Exception {
        copyFile(new File("temp_engine/jython.jar"), new File(PythonUtils.PYTHON_THREAD_PACKAGE_PATH));
        return new File(concat(libPathName, LIB_U_TAG, PYTHON_THREAD_SITE_PACKAGES_PATH));
    }

    protected void copyLibFiles(final String zipFileTempPath, String unzipPath, String libPathName, Log log) throws Exception {
        File libFiles = beforeCopyLibFiles(libPathName);
        if (!libFiles.exists()) {
            log.info("[3]: Can not fund Lib path: {}, create file now >>>", libPathName);
            boolean mkDirs = libFiles.mkdirs();
            log.info("[4]: create file {} succeed: {} >>>", zipFileTempPath, mkDirs);
        } else {
            log.info("[3]: Lib path is exists: {} >>>[4]: Copy after>>>", libPathName);
        }
        log.info("[5]: Start to copy {} from Lib: {} to : {} >>>", PYTHON_THREAD_SITE_PACKAGES_PATH, libFiles.getAbsolutePath(), unzipPath);
        FileUtils.copyToDirectory(libFiles, new File(unzipPath));
    }

    protected void mvJarLibsToLibCachePath(String unzipPath, String libPathName, Log log) throws Exception {
        File jarLib = concatToFile(libPathName, LIB_U_TAG);
        File toPath = concatToFile(PYTHON_THREAD_PACKAGE_PATH, LIB_U_TAG, PYTHON_THREAD_SITE_PACKAGES_PATH);
        if (!toPath.exists() && toPath.mkdirs()) {
            copyFile(jarLib, toPath);
            copyFile(concatToFile(libPathName, PYTHON_SITE_PACKAGES_VERSION_CONFIG), concatToFile(PYTHON_THREAD_PACKAGE_PATH));
            File needDelete = concatToFile(PYTHON_THREAD_PACKAGE_PATH, LIB_U_TAG, "item-packages", PYTHON_THREAD_SITE_PACKAGES_PATH);
            if (needDelete.exists()) {
                FileUtils.deleteDirectory(needDelete);
            }
            log.info("[6]: Copy {} to {}, successfully", PYTHON_THREAD_SITE_PACKAGES_PATH, unzipPath);
        }
    }

    protected int setPackagesResources(Log log, final String zipFileTempPath, String unzipPath, String pyJarPath, String jarName) {
        File file = new File(zipFileTempPath);
        final String libPathName = "temp_engine";
        if (!unZipAsCacheFile(file, zipFileTempPath, libPathName, log)) {
            return -3;
        }
        try {
            copyLibFiles(zipFileTempPath, unzipPath, libPathName, log);
            mvJarLibsToLibCachePath(unzipPath, libPathName, log);
        } catch (Exception e) {
            log.info("[x]: can not prepare Lib for {}, Please manually extract {} and place {}/Lib/ in the {} folder",
                    PYTHON_THREAD_SITE_PACKAGES_PATH,
                    zipFileTempPath,
                    zipFileTempPath,
                    (pyJarPath.endsWith(jarName) ? pyJarPath.replace(jarName, "") : pyJarPath));
            log.warn("Init python resources failed: {}", e.getMessage());
            return -1;
        } finally {
            cleanCache(libPathName, log);
        }
        File packagesFile = getLoopPackagesFile();
        loopPackagesList(packagesFile, log, concat(PYTHON_THREAD_PACKAGE_PATH,PYTHON_THREAD_SITE_PACKAGES_PATH), zipFileTempPath);
        return 1;
    }

    protected void cleanCache(String libPathName, Log log) {
        File temp1 = concatToFile(libPathName);
        if (temp1.exists()) {
            log.info("[*]: Start to clean temp files in {}", libPathName);
            FileUtils.deleteQuietly(temp1);
            log.info("[*]: Temp file cleaned successfully");
        }
    }

    protected RandomAccessFile getRandomAccessFile(File file) throws FileNotFoundException {
        return new RandomAccessFile(file, "rw");
    }
    protected void saveTempZipFile(InputStream inputStream, String savePath, Log log) {
        if (null == inputStream) return;
        try (FileOutputStream outputStream = getFileOutputStream(savePath)) {
            RandomAccessFile file = getRandomAccessFile(new File(savePath));
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
                inputStream.close();
            } catch (IOException e) {
                log.info(e.getMessage());
            }
        }
    }

    protected void copyFile(File file, File target) throws Exception {
        if (null == file || null == target) return;
        File[] files = null;
        if (file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }
        if (!target.exists() || !target.isDirectory()) target.mkdirs();
        if (null == files || files.length <= 0) return;
        for (File f : files) {
            doCopy(f, target);
        }
    }

    protected void doCopy(File file, File target) throws Exception {
        if (null == file || null == target || !file.exists()) return;
        if (file.isDirectory()) {
            copyFile(file, new File(FilenameUtils.concat(target.getPath(), file.getName())));
        } else if (file.isFile()) {
            copy(file, new File(FilenameUtils.concat(target.getPath(), file.getName())));
        }
    }

    protected FileInputStream getFileInputStream(String path) throws FileNotFoundException {
        return new FileInputStream(path);
    }
    protected FileOutputStream getFileOutputStream(String path) throws FileNotFoundException {
        return new FileOutputStream(path);
    }

    protected void copy(File from, File to) throws IOException {
        if (null == from || null == to) return;
        try (FileInputStream fis = getFileInputStream(from.getAbsolutePath());
             FileOutputStream fos = getFileOutputStream(to.getAbsolutePath())) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

    protected Map<String, Object> getPythonConfig(File configFile){
        if (null == configFile || !configFile.exists() || !configFile.isFile()) return new HashMap<>();
        try {
            return (Map<String, Object>) fromJson(FileUtils.readFileToString(configFile, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    protected boolean needSkip(File f) {
        if (null == f) return true;
        if (!f.exists()) return true;
        if (!f.isFile()) return true;
        return !f.getName().contains(PACKAGE_COMPILATION_FILE);
    }

    public String concat(String path, String ...paths){
        for (String s : paths) {
            path = FilenameUtils.concat(path, s);
        }
        return path;
    }

    public File concatToFile(String path, String ...paths) {
        return new File(concat(path, paths));
    }
}
