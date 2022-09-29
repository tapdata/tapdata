package io.tapdata.pdk.core.classloader;

import com.google.common.collect.Lists;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.AnnotationUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Use independent classloader to load each jar and scan the tap nodes in each classloader.
 */
public class ExternalJarManager implements MemoryFetcher {
    private static final String TAG = ExternalJarManager.class.getSimpleName();
    /**
     * jar path
     */
    private String path;

    private List<File> jarFiles;
    /**
     * Whether start a 10 seconds timer to scan jar path for loading new jar at runtime.
     */
    private boolean loadNewJarAtRuntime = false;

    /**
     * The jar already loaded and jar file modification time is different, if false, will not load the changed jar at runtime,
     * otherwise the jar will be reloaded when idle.
     */
    private boolean updateJarWhenIdleAtRuntime = false;

    /**
     * Use for refresh local connector jars
     */
    private boolean refreshLocalJars = false;

    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private JarFoundListener jarFoundListener;
    private JarLoadCompletedListener jarLoadCompletedListener;
    private JarAnnotationHandlersListener jarAnnotationHandlersListener;

    private AtomicBoolean firstTime = new AtomicBoolean(false);

    private ExternalJarManager() {}
    public static ExternalJarManager build() {
        return new ExternalJarManager();
    }
    public ExternalJarManager withPath(String path) {
        this.path = path;
        return this;
    }
    public ExternalJarManager withJarFiles(List<File> jarFiles) {
        this.jarFiles = jarFiles;
        return this;
    }
    public ExternalJarManager withJarAnnotationHandlersListener(JarAnnotationHandlersListener jarAnnotationHandlersListener) {
        this.jarAnnotationHandlersListener = jarAnnotationHandlersListener;
        return this;
    }
    public ExternalJarManager withJarFoundListener(JarFoundListener jarFoundListener) {
        this.jarFoundListener = jarFoundListener;
        return this;
    }
    public ExternalJarManager withJarLoadCompletedListener(JarLoadCompletedListener jarLoadCompletedListener) {
        this.jarLoadCompletedListener = jarLoadCompletedListener;
        return this;
    }
    public ExternalJarManager withLoadNewJarAtRuntime(boolean loadNewJarAtRuntime) {
        this.loadNewJarAtRuntime = loadNewJarAtRuntime;
        return this;
    }
    public ExternalJarManager withUpdateJarWhenIdleAtRuntime(boolean updateJarWhenIdleAtRuntime) {
        this.updateJarWhenIdleAtRuntime = updateJarWhenIdleAtRuntime;
        return this;
    }
    public ExternalJarManager withRefreshLocalJars(boolean refreshLocalJars) {
        this.refreshLocalJars = refreshLocalJars;
        return this;
    }

    public ExternalJarManager start() {
        if (isStarted.compareAndSet(false, true)) {
            if(refreshLocalJars) {
                loadJars();
                loadAtRuntime();
            }
        } else {
            TapLogger.debug(TAG, "Already started");
        }
        return this;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    private void loadAtRuntime() {
        if (loadNewJarAtRuntime || updateJarWhenIdleAtRuntime) {
            int seconds = CommonUtils.getPropertyInt("pdk_load_jar_interval_seconds", 30);
            TapLogger.debug(TAG, "Check jar files every {} seconds for loadNewJarAtRuntime {} updateJarWhenIdleAtRuntime {}", seconds, loadNewJarAtRuntime, updateJarWhenIdleAtRuntime);

            ExecutorsManager.getInstance().getScheduledExecutorService().scheduleAtFixedRate(() -> {
                try {
                    loadJars();
                } catch (Throwable ignored) {}
            }, seconds, seconds, TimeUnit.SECONDS);
        }
    }

    public boolean loadJars() {
        return loadJars(null);
    }
    /**
     * When downloaded new jar, call this method to load new jar immediately
     */
    public synchronized boolean loadJars(String oneJarPath) {
        if(jarAnnotationHandlersListener == null) {
            throw new CoreException(PDKRunnerErrorCodes.PDK_JAR_ANNOTAION_HANDLER_NOT_FOUND, "Jar annotation handlers listener is not specified");
        }
        if(jarFoundListener == null) {
            throw new CoreException(PDKRunnerErrorCodes.PDK_JAR_FOUND_LISTENER_NOT_FOUND, "Jar found listener is not specified");
        }
        File oneJarFile = null;
        if(oneJarPath != null) {
            oneJarFile = new File(oneJarPath);
            if(!oneJarFile.isFile()) {
                throw new CoreException(PDKRunnerErrorCodes.PDK_JAR_FILE_NOT_AVAILABLE_TO_LOAD, "Jar file is not a file or not exists, {}", oneJarFile);
            }
        }

        Collection<File> jars = null;
        File theRunningFolder = null;

        AtomicBoolean theFirstTime = new AtomicBoolean(false);
        if (firstTime.compareAndSet(false, true)) {
            theFirstTime.set(true);
        }

        if(path != null) {
            String runningPath = FilenameUtils.concat(path, "../tap-running/");
            File runningFolder = new File(runningPath);
            theRunningFolder = runningFolder;

            if(theFirstTime.get()) {
                CommonUtils.ignoreAnyError(() -> {
                    if (runningFolder.exists()) {
                        FileUtils.forceDelete(runningFolder);
                        FileUtils.forceMkdir(runningFolder);
                    }
                }, TAG);
            }
        }

        if(oneJarFile != null) {
            jars = new ArrayList<>();
            jars.add(oneJarFile);
        } else if(path != null) {
//            logger.error("Load jars failed as path is null");
//            return false;
//        }
            File sourcePath = new File(path);
            if (!sourcePath.isDirectory()) {
                TapLogger.error(TAG, "Path {} is not a directory, external jars suppose to be in the directory", path);
            }

            jars = new ArrayList<>(FileUtils.listFiles(sourcePath,
                    FileFilterUtils.suffixFileFilter(".jar"),
                    FileFilterUtils.directoryFileFilter()));
        } else if(jarFiles != null && !jarFiles.isEmpty()) {
            jars = jarFiles;
        } else {
            throw new CoreException(PDKRunnerErrorCodes.PDK_NO_FILES_FOUND_WHEN_LOAD_JARS, "Load jars failed as path is null or jarFiles are null or oneJarFile is null");
        }
        File finalTheRunningFolder = theRunningFolder;
        jars.forEach(jar -> {
            Throwable error = null;
            DependencyURLClassLoader dependencyURLClassLoader = null;
            boolean ignored = false;
            try {
                if(jarFoundListener == null || !jarFoundListener.needReloadJar(jar, theFirstTime.get())) {
                    ignored = true;
                    return;
                }
                File targetJarFile = jar;
                if(finalTheRunningFolder != null) {
                    //copy to rename jar to avoid some times load resource failed issue.
                    if(!finalTheRunningFolder.exists()) {
                        FileUtils.forceMkdir(finalTheRunningFolder);
                    } else if(!finalTheRunningFolder.isDirectory()) {
                        TapLogger.debug(TAG, "tap-running is not a directory, will delete it, create a directory again");
                        FileUtils.forceDelete(finalTheRunningFolder);
                        FileUtils.forceMkdir(finalTheRunningFolder);
                    } else {
//                        FileUtils.forceDelete(finalTheRunningFolder);
//                        FileUtils.forceMkdir(finalTheRunningFolder);
                    }

                    String fileNameWithoutExtension = jar.getName().substring(0, jar.getName().length() - ".jar".length());
                    targetJarFile = new File(finalTheRunningFolder.getAbsolutePath() + File.separator + fileNameWithoutExtension + "_" + UUID.randomUUID().toString() + ".jar");
                    FileUtils.copyFile(jar, targetJarFile);
                }
                List<URL> urls = Lists.newArrayList(targetJarFile.toURI().toURL());
                dependencyURLClassLoader = new DependencyURLClassLoader(urls);
                Reflections reflections = new Reflections(new ConfigurationBuilder()
                        .addScanners(new TypeAnnotationsScanner())
                        .filterInputsBy(new FilterBuilder()
//                                .include("^.*\\.class$")
                                        .includePackage("io", "pdk")
//                                .exclude("^.*module-info.class$")
                        )
                        .setUrls(urls)
                        .addClassLoader(dependencyURLClassLoader.getActualClassLoader()));
                TapLogger.debug(TAG, "Analyze jar file {}", targetJarFile.getAbsolutePath());
                TapLogger.debug(TAG, "Tapdata SDK will only scan classes under package 'io' or 'pdk', please ensure your annotated classes are following this rule. ");
                AnnotationUtils.runClassAnnotationHandlers(reflections, jarAnnotationHandlersListener.annotationHandlers(jar), TAG);

//                Set<Class<?>> connectorClasses = reflections.getTypesAnnotatedWith(OpenAPIConnector.class, true);
            } catch (MalformedURLException e) {
                error = e;
                TapLogger.error(TAG, "MalformedURL {} while load jar, error {}", jar.getAbsolutePath(), e.getMessage());
            } catch (Throwable throwable) {
                error = throwable;
                TapLogger.error(TAG, "Unknown error while load jar {}, error {}", jar.getAbsolutePath(), throwable.getMessage());
            } finally {
                if(!ignored) {
                    Throwable finalError = error;
                    DependencyURLClassLoader finalDependencyURLClassLoader = dependencyURLClassLoader;
                    CommonUtils.ignoreAnyError(() -> {
                        if(jarLoadCompletedListener != null) {
                            jarLoadCompletedListener.loadCompleted(jar, finalDependencyURLClassLoader, finalError);
                        }
                    }, TAG);
                }
            }
        });
        return true;
    }

    public static void main(String... args) {
        List<URL> urls = new ArrayList<>();
        File sourcePath = new File("/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist");
        if (sourcePath.exists() && sourcePath.isDirectory()) {
            Collection<File> jars = new ArrayList<>(FileUtils.listFiles(sourcePath,
                    FileFilterUtils.suffixFileFilter(".jar"),
                    FileFilterUtils.directoryFileFilter()));
            jars.forEach(jar -> {
                String path = "jar:file://" + jar.getAbsolutePath() + "!/";
                try {
                    urls.add(jar.toURI().toURL());
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                    TapLogger.debug(TAG, "MalformedURL {} while load jars, error {}", path, e.getMessage());
                }
            });
            if (!urls.isEmpty()) {
                DependencyURLClassLoader dependencyURLClassLoader = new DependencyURLClassLoader(urls);
//                Reflections reflections = new Reflections(new ConfigurationBuilder()
//                        .addScanners(new TypeAnnotationsScanner())
//                        .addScanners(new SubTypesScanner(false))
//                        .setUrls(urls)
////                        .forPackages("io")
//                        .addClassLoader(dependencyURLClassLoader));
              Reflections reflections = new Reflections(new ConfigurationBuilder()
                  .addScanners(new TypeAnnotationsScanner())
                  .filterInputsBy(new FilterBuilder()
//                                .include("^.*\\.class$")
                          .includePackage("io", "pdk")
//                                .exclude("^.*module-info.class$")
                  )
                  .setUrls(urls)
                  .addClassLoader(dependencyURLClassLoader.getActualClassLoader()));
                Set<Class<?>> connectorClasses = reflections.getTypesAnnotatedWith(TapConnectorClass.class, true);
//                System.out.println("connectorClasses " + connectorClasses);
            }
        }
    }

    public boolean isLoadNewJarAtRuntime() {
        return loadNewJarAtRuntime;
    }

    public boolean isUpdateJarWhenIdleAtRuntime() {
        return updateJarWhenIdleAtRuntime;
    }

    public boolean isRefreshLocalJars() {
        return refreshLocalJars;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("Path", path)
                .kv("JarFiles", jarFiles != null ? Arrays.toString(jarFiles.toArray()) : null)
                .kv("LoadNewJarAtRuntime", loadNewJarAtRuntime)
                .kv("UpdateJarWhenIdleAtRuntime", updateJarWhenIdleAtRuntime)
                .kv("RefreshLocalJars", refreshLocalJars)
                .kv("IsStarted", isStarted)
                ;
    }
}
