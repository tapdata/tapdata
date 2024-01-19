package io.tapdata.observable.metric.py;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.simplify.TapSimplify;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;
import org.springframework.cglib.transform.AbstractClassLoader;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PythonUtilsTest {
    PythonUtils utils;
    Log log;
    @BeforeEach
    void init() {
        utils = mock(PythonUtils.class);
        when(utils.concat(anyString(), any(String[].class))).thenReturn("mock-concat");
        log = mock(Log.class);
        doNothing().when(log).warn(anyString(), any(Object[].class));
        doNothing().when(log).warn(anyString());
        doNothing().when(log).warn(any(Object.class));
        doNothing().when(log).warn(any(Character.class));

        doNothing().when(log).info(anyString(), any(Object[].class));
        doNothing().when(log).info(anyString());
        doNothing().when(log).info(any(Object.class));
        doNothing().when(log).info(any(Character.class));

        doNothing().when(log).debug(anyString(), any(Object[].class));
        doNothing().when(log).debug(anyString());

        doNothing().when(log).error(anyString(), any(Object[].class));
        doNothing().when(log).error(anyString());
        doNothing().when(log).error(any(Object.class));
        doNothing().when(log).error(any(Character.class));
        doNothing().when(log).error(anyString(), any(Throwable.class));
    }

    @Nested
    class CreateTest {
        @Test
        void testCreate() {
            PythonUtils utils = PythonUtils.create();
            Assertions.assertNotNull(utils);
        }
    }

    @Nested
    class FlowTest {
        @BeforeEach
        void init() {
            doNothing().when(utils).flowStart(any(Log.class));
        }

        @Test
        void testFlowNormal() {
            TapLog tapLog = mock(TapLog.class);
            try (
                    MockedStatic<PythonUtils> appTypeMockedStatic = mockStatic(PythonUtils.class);
            ) {
                appTypeMockedStatic.when(() -> {
                    PythonUtils.flow(any(TapLog.class));
                }).thenCallRealMethod();
                appTypeMockedStatic.when(PythonUtils::create).thenReturn(utils);
                PythonUtils.flow(tapLog);
                appTypeMockedStatic.verify(PythonUtils::create, times(1));
            } finally {
                verify(utils, times(1)).flowStart(any(Log.class));
            }
        }

        @Test
        void testFlowNullLog() {
            try (
                    MockedStatic<PythonUtils> appTypeMockedStatic = mockStatic(PythonUtils.class);
            ) {
                appTypeMockedStatic.when(() -> {
                    PythonUtils.flow(null);
                }).thenCallRealMethod();
                appTypeMockedStatic.when(PythonUtils::create).thenReturn(utils);
                Assertions.assertThrows(IllegalArgumentException.class, () -> {
                    PythonUtils.flow(null);
                });
                appTypeMockedStatic.verify(PythonUtils::create, times(0));
            } finally {
                verify(utils, times(0)).flowStart(null);
            }
        }
    }

    @Nested
    class VariableTest {
        @Test
        void testVariable() {
            Assertions.assertEquals("cd %s; java -jar %s setup.py install", PythonUtils.PACKAGE_COMPILATION_COMMAND);
            Assertions.assertEquals("setup.py", PythonUtils.PACKAGE_COMPILATION_FILE);
            Assertions.assertEquals("py-lib", PythonUtils.PYTHON_THREAD_PACKAGE_PATH);
            Assertions.assertEquals("site-packages", PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH);
            Assertions.assertEquals("jython-standalone-2.7.3.jar", PythonUtils.PYTHON_THREAD_JAR);
            Assertions.assertEquals("install.json", PythonUtils.PYTHON_SITE_PACKAGES_VERSION_CONFIG);
            Assertions.assertEquals("agent", PythonUtils.AGENT_TAG);
            Assertions.assertEquals("BOOT-INF", PythonUtils.BOOT_INF_TAG);
            Assertions.assertEquals("lib", PythonUtils.LIB_TAG);
            Assertions.assertEquals("Lib", PythonUtils.LIB_U_TAG);
            Assertions.assertEquals("PythonUtils", PythonUtils.TAG);
        }
    }

    @Nested
    class FlowStartTest {

        @BeforeEach
        void init() {
            doCallRealMethod().when(utils).flowStart(any(Log.class));
            when(utils.setPackagesResources(any(Log.class), anyString(), anyString(), anyString(), anyString())).thenReturn(0);
            doNothing().when(utils).unzipPythonStandalone(any(Log.class));
            when(utils.execute(anyString(), anyString(), any(Log.class))).thenReturn(0);
            doNothing().when(utils).deleteFile(any(File.class), any(Log.class));
        }

        @Test
        void testFlowStartNormal() {
            when(utils.unzipIeJar()).thenReturn(true);
            utils.flowStart(log);
            assertCommon();
            verify(utils, times(0)).execute(anyString(), anyString(), any(Log.class));
            verify(utils, times(1)).unzipPythonStandalone(log);
            verify(utils, times(1)).setPackagesResources(any(Log.class), anyString(), anyString(), anyString(), anyString());
        }

        @Test
        void testFlowStartNotUnzipIeJar() {
            when(utils.unzipIeJar()).thenReturn(false);
            utils.flowStart(log);
            assertCommon();
            verify(utils, times(1)).execute(anyString(), anyString(), any(Log.class));
            verify(utils, times(0)).unzipPythonStandalone(log);
            verify(utils, times(0)).setPackagesResources(any(Log.class), anyString(), anyString(), anyString(), anyString());
        }

        void assertCommon() {
            verify(utils, times(1)).unzipIeJar();
            verify(utils, times(2)).deleteFile(any(File.class), any(Log.class));
        }
    }

    @Nested
    class DeleteFileTest {
        File file;
        @BeforeEach
        void init() throws IOException {
            file = mock(File.class);
            when(file.getAbsolutePath()).thenReturn("mock.path");
            doCallRealMethod().when(utils).deleteFile(any(File.class), any(Log.class));
        }

        /**file exist and isDirectory*/
        @Test
        void testDeleteFileWithFileNotExist() {
            when(file.exists()).thenReturn(true);
            when(file.isDirectory()).thenReturn(true);
            assertMockFunction(1, 1, 0);
        }

        /**file not exist*/
        @Test
        void testDeleteFileWithFileExistAndIsDirectory() {
            when(file.exists()).thenReturn(false);
            assertMockFunction(1, 0, 0);
        }

        /**file notDirectory*/
        @Test
        void testDeleteFileWithFileExistButNotDirectory() {
            when(file.exists()).thenReturn(true);
            when(file.isDirectory()).thenReturn(false);
            assertMockFunction(1, 1, 0);
        }

        /**throw exception*/
        @Test
        void testDeleteFileWithFileExistButThrow() {
            when(file.exists()).thenReturn(true);
            when(file.isDirectory()).then(m ->{throw new IllegalArgumentException("xxx");});
            assertMockFunction(1, 1, 1);
        }

        void assertMockFunction(int existsTimes, int isDirectoryTimes, int infoTimes) {
            try (MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class)) {
                fileUtils.when(() -> FileUtils.deleteDirectory(any(File.class))).thenAnswer((Answer<Void>) invocation -> null);
                fileUtils.when(() -> FileUtils.delete(any(File.class))).thenAnswer((Answer<Void>) invocation -> null);
                utils.deleteFile(file, log);
            } finally {
                verify(file, times(existsTimes)).exists();
                verify(file, times(isDirectoryTimes)).isDirectory();
                verify(log, times(infoTimes)).info(anyString(), anyString(), anyString());
            }
        }

//        @Test
//        public void testDeleteFileOfDirectory() {
//            File file = new File("temp");
//            if (file.mkdir() && file.exists()) {
//                utils.deleteFile(file, new TapLog());
//            }
//            Assertions.assertFalse(file.exists());
//        }
//
//        @Test
//        public void testDeleteFileOfFile() {
//            File file = new File("temp.txt");
//            try {
//                if (file.createNewFile() && file.exists()) {
//                    utils.deleteFile(file, new TapLog());
//                    Assertions.assertFalse(file.exists());
//                }
//            } catch (IOException ignore) { }
//        }

    }

    @Nested
    class UnzipPythonStandaloneTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(utils).unzipPythonStandalone(any(Log.class));
        }

        /**do copyFile function which by mock*/
        @SneakyThrows
        @Test
        void testUnzipPythonStandaloneNormal() {
            doNothing().when(utils).copyFile(any(File.class), any(File.class));
            utils.unzipPythonStandalone(log);
            assertFunction(1, 0);
        }

        /**do copyFile function which by mock, but throw exception*/
        @SneakyThrows
        @Test
        void testUnzipPythonStandaloneDoCopyFileButThrow() {
            Exception mock = mock(Exception.class);
            when(mock.getMessage()).thenReturn("mock exception");
            doAnswer(m-> {throw mock;}).when(utils).copyFile(any(File.class), any(File.class));
            utils.unzipPythonStandalone(log);
            assertFunction(1, 1);
        }

        @SneakyThrows
        void assertFunction(int copyFileTimes, int logTimes) {
            verify(utils, times(copyFileTimes)).copyFile(any(File.class), any(File.class));
            verify(log, times(logTimes)).warn(anyString(), anyString(), anyString(), anyString());
        }
    }

    @Nested
    class GetLoopPackagesFileTest {
        @BeforeEach
        void init() {
            when(utils.getLoopPackagesFile()).thenCallRealMethod();
        }
        @Test
        void testGetLoopPackagesFile() {
            File file = utils.getLoopPackagesFile();
            Assertions.assertNotNull(file);
        }
    }

    @Nested
    class LoopByConfigTest {
        File configFile;
        Map<String, Object> configMap;
        Collection<Object> packages;
        @BeforeEach
        void init() {
            configFile = mock(File.class);
            packages = new ArrayList<>();
            packages.add("install-package-mock");
            configMap = mock(Map.class);
            when(configMap.get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(packages);
            when(utils.loopByConfig(any(File.class), any(Log.class), anyString(), anyString())).thenCallRealMethod();
            when(utils.getPythonConfig(any(File.class))).thenReturn(configMap);
            doNothing().when(utils).loopFiles(anyList(), any(Log.class), anyString());
        }

        @Test
        void testLoopByConfigNormal() {
            try (MockedStatic<TapSimplify> mockedStatic = mockStatic(TapSimplify.class)) {
                mockedStatic.when(() -> TapSimplify.toJson(any(Object.class))).thenReturn("mokc json");
                boolean byConfig = utils.loopByConfig(configFile, log, "mock", "python-jar-path");
                mockedStatic.verify(() -> TapSimplify.toJson(any(Map.class)), times(1));
                Assertions.assertTrue(byConfig);
            } finally {
                assertVerify(1, 1, 1, 1);
            }
        }

        @Test
        void testLoopByConfigConfigValueNotCollection() {
            try (MockedStatic<TapSimplify> mockedStatic = mockStatic(TapSimplify.class)) {
                mockedStatic.when(() -> TapSimplify.toJson(any(Object.class))).thenReturn("mokc json");
                when(configMap.get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn("string values");
                boolean byConfig = utils.loopByConfig(configFile, log, "mock", "python-jar-path");
                mockedStatic.verify(() -> TapSimplify.toJson(any(Map.class)), times(0));
                Assertions.assertFalse(byConfig);
            } finally {
                assertVerify(1, 1, 0, 0);
            }
        }

        @Test
        void testLoopByConfigConfigValueIsEmptyCollection() {
            try (MockedStatic<TapSimplify> mockedStatic = mockStatic(TapSimplify.class)) {
                mockedStatic.when(() -> TapSimplify.toJson(any(Object.class))).thenReturn("mokc json");
                packages = new ArrayList<>();
                when(configMap.get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(packages);
                boolean byConfig = utils.loopByConfig(configFile, log, "mock", "python-jar-path");
                mockedStatic.verify(() -> TapSimplify.toJson(any(Map.class)), times(0));
                Assertions.assertFalse(byConfig);
            } finally {
                assertVerify(1, 1, 0, 0);
            }
        }

        @Test
        void testLoopByConfigConfigValueIsCollectionButAllElementNotString() {
            try (MockedStatic<TapSimplify> mockedStatic = mockStatic(TapSimplify.class)) {
                mockedStatic.when(() -> TapSimplify.toJson(any(Object.class))).thenReturn("mokc json");
                when(utils.getPythonConfig(any(File.class))).thenReturn(configMap);
                packages = new ArrayList<>();
                packages.add("mock");
                packages.add(100);
                when(configMap.get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(packages);
                boolean byConfig = utils.loopByConfig(configFile, log, "mock", "python-jar-path");
                mockedStatic.verify(() -> TapSimplify.toJson(any(Map.class)), times(1));
                Assertions.assertTrue(byConfig);
            } finally {
                assertVerify(1, 1, 1, 1);
            }
        }
        @Test
        void testLoopByConfigConfigValueIsCollectionButElementsContainsNullString() {
            try (MockedStatic<TapSimplify> mockedStatic = mockStatic(TapSimplify.class)) {
                mockedStatic.when(() -> TapSimplify.toJson(any(Object.class))).thenReturn("mokc json");
                packages = new ArrayList<>();
                packages.add("mock");
                packages.add(null);
                when(configMap.get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(packages);
                boolean byConfig = utils.loopByConfig(configFile, log, "mock", "python-jar-path");
                mockedStatic.verify(() -> TapSimplify.toJson(any(Object.class)), times(1));
                Assertions.assertTrue(byConfig);
            } finally {
                assertVerify(1, 1, 1,  1);
            }
        }
        @Test
        void testLoopByConfigConfigValueIsCollectionButAllElementsAreNullString() {
            try (MockedStatic<TapSimplify> mockedStatic = mockStatic(TapSimplify.class)) {
                mockedStatic.when(() -> TapSimplify.toJson(any(Object.class))).thenReturn("mokc json");
                packages = new ArrayList<>();
                packages.add(null);
                when(configMap.get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(packages);
                boolean byConfig = utils.loopByConfig(configFile, log, "mock", "python-jar-path");
                mockedStatic.verify(() -> TapSimplify.toJson(any(Object.class)), times(1));
                Assertions.assertFalse(byConfig);
            } finally {
                assertVerify(1, 1, 0, 1);
            }
        }

        void assertVerify(int getPythonConfigTimes, int getTimes, int loopFilesTimes, int infoTimes) {
            verify(utils, times(getPythonConfigTimes)).getPythonConfig(configFile);
            verify(configMap, times(getTimes)).get(PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH);
            verify(log, times(infoTimes)).info(anyString(), anyString());
            verify(utils, times(loopFilesTimes)).loopFiles(anyList(), any(Log.class), anyString());
        }
    }

    @Nested
    class LoopPackagesListTest {
        File config;
        @BeforeEach
        void init() {
            config = mock(File.class);

            when(config.exists()).thenReturn(true);
            when(config.isFile()).thenReturn(true);

            when(utils.loopByConfig(any(File.class), any(Log.class), anyString(), anyString())).thenReturn(false);
            doNothing().when(utils).doLoopStart(any(File.class), anyString(), any(Log.class));
            doCallRealMethod().when(utils).loopPackagesList(any(File.class), any(Log.class), anyString(), anyString());
        }

        @Test
        void testLoopPackagesListNormal() {
            utils.loopPackagesList(config, log, "loopPath", "pythonJarPath");
            assertVerify(1, 1, 1, 1);
        }

        @Test
        void testLoopPackagesListConfigIsNull() {
            doCallRealMethod().when(utils).loopPackagesList(null, log, "loopPath", "pythonJarPath");
            utils.loopPackagesList(null, log, "loopPath", "pythonJarPath");
            assertVerify(0, 0, 0, 0);
        }

        @Test
        void testLoopPackagesListConfigFileNotExists() {
            when(config.exists()).thenReturn(false);
            utils.loopPackagesList(config, log, "loopPath", "pythonJarPath");
            assertVerify(1, 0, 0, 1);
        }

        @Test
        void testLoopPackagesListConfigFileNotFile() {
            when(config.isFile()).thenReturn(false);
            utils.loopPackagesList(config, log, "loopPath", "pythonJarPath");
            assertVerify(1, 1, 0, 1);
        }

        @Test
        void testLoopPackagesListLoopByConfigIsTrue() {
            when(utils.loopByConfig(any(File.class), any(Log.class), anyString(), anyString())).thenReturn(true);
            utils.loopPackagesList(config, log, "loopPath", "pythonJarPath");
            assertVerify(1, 1, 1, 0);
        }

        void assertVerify(int existsTimes, int isFileTimes, int loopByConfigTimes, int doLoopStartTimes) {
            verify(config, times(existsTimes)).exists();
            verify(config, times(isFileTimes)).isFile();
            verify(utils, times(loopByConfigTimes)).loopByConfig(any(File.class), any(Log.class), anyString(), anyString());
            verify(utils, times(doLoopStartTimes)).doLoopStart(any(File.class), anyString(), any(Log.class));
        }

    }

    @Nested
    class DoLoopStartTest {
        File loopFile;
        @BeforeEach
        void init() {
            loopFile = mock(File.class);
            when(loopFile.listFiles()).thenReturn(new File[]{mock(File.class)});

            doNothing().when(utils).loopFiles(anyList(), any(Log.class), anyString());
            doCallRealMethod().when(utils).doLoopStart(any(File.class), anyString(), any(Log.class));
        }

        @Test
        void testDoLoopStartNormal() {
            utils.doLoopStart(loopFile, "pythonJarPath", log);
            assertVerify(1, 1);
        }

        @Test
        void testDoLoopStartListFilesIsNull() {
            when(loopFile.listFiles()).thenReturn(null);
            utils.doLoopStart(loopFile, "pythonJarPath", log);
            assertVerify(1, 0);
        }

        void assertVerify(int listFilesTimes, int loopFilesTimes) {
            verify(loopFile, times(listFilesTimes)).listFiles();
            verify(utils, times(loopFilesTimes)).loopFiles(anyList(), any(Log.class), anyString());
        }
    }

    @Nested
    class LoopFilesTest {
        List<File> loopFiles;
        File loopFile;
        @BeforeEach
        void init() {
            loopFile = mock(File.class);
            loopFiles = new ArrayList<>();
            loopFiles.add(loopFile);

            doCallRealMethod().when(utils).loopFiles(anyList(), any(Log.class), anyString());
            when(utils.getSetUpPyFile(any(File.class))).thenReturn(mock(File.class));
            doNothing().when(utils).unPackageBySetUpPy(any(File.class), any(File.class), anyString(), any(Log.class));
        }

        @Test
        void testLoopFilesNormal() {
            assertVerify(loopFiles, 1, 1, 1);
        }

        @Test
        void testLoopFilesNullLoopFiles() {
            doCallRealMethod().when(utils).loopFiles(null, log, "pythonJarPath");
            assertVerify(null, 0, 0, 0);
        }

        @Test
        void testLoopFilesEmptyLoopFiles() {
            assertVerify(new ArrayList<>(), 0, 0, 0);
        }

        @Test
        void testLoopFilesLoopFilesButElementHasNullFile() {
            loopFiles.add(null);
            assertVerify(loopFiles, 1, 1, 1);
        }

        void assertVerify(List<File> loopFileList, int getSetUpPyFileTimes, int unPackageBySetUpPyTimes, int fixFileTimes) {
            try (
                    MockedStatic<FixFileUtil> appTypeMockedStatic = mockStatic(FixFileUtil.class);
            ) {
                appTypeMockedStatic.when(() -> {
                    FixFileUtil.fixFile(any(File.class));
                }).thenReturn(loopFile);
                utils.loopFiles(loopFileList, log, "pythonJarPath");
                appTypeMockedStatic.verify(() -> {
                    FixFileUtil.fixFile(any(File.class));
                }, times(fixFileTimes));
            } finally {
                verify(utils, times(getSetUpPyFileTimes)).getSetUpPyFile(any(File.class));
                verify(utils, times(unPackageBySetUpPyTimes)).unPackageBySetUpPy(any(File.class), any(File.class), anyString(), any(Log.class));
            }
        }
    }

    @Nested
    class GetSetUpPyFileTest {
        File parentFile;
        @BeforeEach
        void init() {
            parentFile = mock(File.class);
            when(parentFile.getAbsolutePath()).thenReturn("mock-path");
            when(utils.getSetUpPyFile(any(File.class))).thenCallRealMethod();
        }

        @Test
        void testGetSetUpPyFile() {
            File pyFile = utils.getSetUpPyFile(parentFile);
            Assertions.assertNotNull(pyFile);
        }
    }

    @Nested
    class UnPackageBySetUpPyTest {
        File afterUnzipFile;
        String pythonJarPath;
        File setUpPyFile;
        @BeforeEach
        void init() {
            afterUnzipFile = mock(File.class);
            pythonJarPath = "mock-string";
            setUpPyFile = mock(File.class);
            doNothing().when(utils).unPackageFile(any(File.class), any(File.class), anyString(), any(Log.class));
            doCallRealMethod().when(utils).unPackageBySetUpPy(setUpPyFile, afterUnzipFile, pythonJarPath, log);
        }

        @Test
        void testUnPackageBySetUpPyNormal1() {
            when(setUpPyFile.exists()).thenReturn(true);
            when(setUpPyFile.isFile()).thenReturn(true);
            utils.unPackageBySetUpPy(setUpPyFile, afterUnzipFile, pythonJarPath, log);
            assertVerify(1, 1, 1);
        }
        @Test
        void testUnPackageBySetUpPyNormal2() {
            when(setUpPyFile.exists()).thenReturn(true);
            when(setUpPyFile.isFile()).thenReturn(false);
            utils.unPackageBySetUpPy(setUpPyFile, afterUnzipFile, pythonJarPath, log);
            assertVerify(1, 1, 0);
        }
        @Test
        void testUnPackageBySetUpPyNormal3() {
            when(setUpPyFile.exists()).thenReturn(false);
            when(setUpPyFile.isFile()).thenReturn(true);
            utils.unPackageBySetUpPy(setUpPyFile, afterUnzipFile, pythonJarPath, log);
            assertVerify(1, 0, 0);
        }
        @Test
        void testUnPackageBySetUpPyNormal4() {
            when(setUpPyFile.exists()).thenReturn(false);
            when(setUpPyFile.isFile()).thenReturn(false);
            utils.unPackageBySetUpPy(setUpPyFile, afterUnzipFile, pythonJarPath, log);
            assertVerify(1, 0, 0);
        }
        @Test
        void testUnPackageBySetUpPyNormal5() {
            doCallRealMethod().when(utils).unPackageBySetUpPy(null, afterUnzipFile, pythonJarPath, log);
            utils.unPackageBySetUpPy(null, afterUnzipFile, pythonJarPath, log);
            assertVerify(0, 0, 0);
        }

        void assertVerify(int existsTimes, int isFileTimes, int unPackageFileTimes) {
            verify(setUpPyFile, times(existsTimes)).exists();
            verify(setUpPyFile, times(isFileTimes)).isFile();
            verify(utils, times(unPackageFileTimes)).unPackageFile(any(File.class), any(File.class), anyString(), any(Log.class));
        }
    }

    @Nested
    class UnPackageFileTest {
        File unPackageFile;
        File afterUnzipFile;
        String pythonJarPath;
        ProcessBuilder processBuilder;
        Process start;
        File setUpParentFile;
        //Thread mockThread;
        InputStream infoStream;
        InputStream errorStream;

        @SneakyThrows
        @BeforeEach
        void init() {
            infoStream = mock(InputStream.class);
            errorStream = mock(InputStream.class);
            //mockThread = mock(Thread.class);
            //doNothing().when(mockThread).interrupt();
            setUpParentFile = mock(File.class);
            unPackageFile = mock(File.class);
            when(unPackageFile.getParentFile()).thenReturn(setUpParentFile);
            when(setUpParentFile.getAbsolutePath()).thenReturn("mock-AbsolutePath");

            afterUnzipFile = mock(File.class);
            when(afterUnzipFile.getName()).thenReturn("mock-name");

            pythonJarPath = "pythonJarPath";

            start = mock(Process.class);
            doNothing().when(start).destroy();

            processBuilder = mock(ProcessBuilder.class);

            doCallRealMethod().when(utils).unPackageFile(any(File.class), any(File.class), anyString(), any(Log.class));
            when(start.getInputStream()).thenReturn(infoStream);
            when(start.getErrorStream()).thenReturn(errorStream);
            doNothing().when(utils).printInfo(infoStream, log, "mock-name");
            doNothing().when(utils).printInfo(errorStream, log, "mock-name");
            when(utils.getUnPackageFileProcessBuilder(anyString(), anyString())).thenReturn(processBuilder);
        }

        @SneakyThrows
        @Test
        void testUnPackageFileNormal() {
            when(start.waitFor()).thenReturn(1);
            when(processBuilder.start()).thenReturn(start);
            assertVerify(0, 1, 2, 1, 1, 1, 1, 2, 0, 0);
        }

        @SneakyThrows
        @Test
        void testUnPackageFileStartFunctionThrowIOException() {
            IOException exception = mock(IOException.class);
            when(exception.getMessage()).thenReturn("mock-message");
            when(processBuilder.start()).thenThrow(exception);
            assertVerify(0, 1, 1, 1, 0, 0, 1, 1, 0 , 1, 0);
            verify(exception, times(1)).getMessage();
        }

        @SneakyThrows
        @Test
        void testUnPackageFileWaitForFunctionThrowInterruptedException() {
            when(processBuilder.start()).thenReturn(start);
            InterruptedException exception = mock(InterruptedException.class);
            when(exception.getMessage()).thenReturn("mock-message");
            when(start.waitFor()).thenThrow(exception);
            assertVerify(1, 1, 1, 1, 1, 1, 1, 1, 1 , 0);
            verify(exception, times(1)).getMessage();
        }

        @SneakyThrows
        void assertVerify(int currentThreadTimes,
                          int getAbsolutePathTimes,
                          int getNameTimes,
                          int startTimes,
                          int waitForTimes,
                          int destroyTimes,
                          int getUnPackageFileProcessBuilderTimes,
                          int logInfoTimes,
                          int logWarnATimes,
                          int logWarnBTimes ) {
            assertVerify(currentThreadTimes,
                    getAbsolutePathTimes,
                    getNameTimes,
                    startTimes,
                    waitForTimes,
                    destroyTimes,
                    getUnPackageFileProcessBuilderTimes,
                    logInfoTimes,
                    logWarnATimes,
                    logWarnBTimes, startTimes);
        }
        @SneakyThrows
        void assertVerify(int currentThreadTimes,
                          int getAbsolutePathTimes,
                          int getNameTimes,
                          int startTimes,
                          int waitForTimes,
                          int destroyTimes,
                          int getUnPackageFileProcessBuilderTimes,
                          int logInfoTimes,
                          int logWarnATimes,
                          int logWarnBTimes, int printTimes) {
            utils.unPackageFile(unPackageFile, afterUnzipFile, pythonJarPath, log);

            //            try (MockedStatic<Thread> mockedStatic = mockStatic(Thread.class)) {
//                mockedStatic.when(Thread::currentThread).thenReturn(mockThread);
//                utils.unPackageFile(unPackageFile, afterUnzipFile, pythonJarPath, log);
//                mockedStatic.verify(Thread::currentThread, times(currentThreadTimes));
//            } finally {
            verify(afterUnzipFile, times(getNameTimes)).getName();
            verify(utils, times(getUnPackageFileProcessBuilderTimes)).getUnPackageFileProcessBuilder(anyString(), anyString());
            verify(unPackageFile, times(getAbsolutePathTimes)).getParentFile();
            verify(setUpParentFile, times(getAbsolutePathTimes)).getAbsolutePath();
            verify(processBuilder, times(startTimes)).start();
            verify(start, times(waitForTimes)).waitFor();
            verify(start, times(destroyTimes)).destroy();
            //verify(mockThread, times(currentThreadTimes)).interrupt();
            verify(log, times(logInfoTimes)).info(anyString(), anyString());
            verify(log, times(logWarnATimes)).warn(anyString());
            verify(log, times(logWarnBTimes)).warn(anyString(), anyString());
//            }
            verify(start, times(printTimes)).getInputStream();
            verify(utils, times(printTimes)).printInfo(infoStream, log, "mock-name");
            verify(start, times(printTimes)).getErrorStream();
            verify(utils, times(printTimes)).printInfo(errorStream, log, "mock-name");
        }
    }

    @Nested
    class GetUnPackageFileProcessBuilderTest {
        String unPackageAbsolutePath;
        String pythonJarPath;
        @BeforeEach
        void init() {
            unPackageAbsolutePath = "unPackageAbsolutePath";
            pythonJarPath = "pythonJarPath";
        }

        @Test
        void testGetUnPackageFileProcessBuilderNormal() {
            when(utils.getUnPackageFileProcessBuilder(anyString(), anyString())).thenCallRealMethod();
            ProcessBuilder processBuilder = utils.getUnPackageFileProcessBuilder(unPackageAbsolutePath, pythonJarPath);
            Assertions.assertNotNull(processBuilder);
        }

        @Test
        void testGetUnPackageFileProcessBuilderNullPythonJarPath() {
            when(utils.getUnPackageFileProcessBuilder(unPackageAbsolutePath, null)).thenCallRealMethod();
            Assertions.assertThrows(CoreException.class, () -> {
                utils.getUnPackageFileProcessBuilder(unPackageAbsolutePath, null);
            });
        }
    }

    @Nested
    class GetCurrentThreadContextClassLoaderTest {
        @BeforeEach
        void init() {
            when(utils.getCurrentThreadContextClassLoader()).thenCallRealMethod();
        }

        @Test
        void testGetCurrentThreadContextClassLoader() {
            Assertions.assertNotNull(utils.getCurrentThreadContextClassLoader());
        }
    }

    @Nested
    class GetLibPathTest {
        ClassLoader classLoader;
        URL[] urls;
        URL url;
        InputStream stream;
        String jarName;
        AtomicReference<String> ato;
        String mockPath;

        @SneakyThrows
        @BeforeEach
        void init() {
            ato = new AtomicReference<>();
            jarName = "mock";
            mockPath = "mock-path";

            stream = mock(InputStream.class);

            url = mock(URL.class);

            when(url.getPath()).thenReturn(mockPath);
            when(url.openStream()).thenReturn(stream);

            urls = new URL[]{url};
        }

        @SneakyThrows
        @Test
        void testGetLibPathNormal() {
            when(utils.getLibPath(jarName, ato)).thenCallRealMethod();
            classLoader = mock(URLClassLoader.class);
            when(utils.getCurrentThreadContextClassLoader()).thenReturn(classLoader);
            when(((URLClassLoader)classLoader).getURLs()).thenReturn(urls);
            InputStream libPath = utils.getLibPath(jarName, ato);
            Assertions.assertNotNull(libPath);
            Assertions.assertEquals(stream, libPath);
            Assertions.assertEquals(mockPath, ato.get());
            verify((URLClassLoader)classLoader, times(1)).getURLs();
            assertVerify(1, 1, 1);
        }

        @SneakyThrows
        @Test
        void testGetLibPathNotURLClassLoader() {
            when(utils.getLibPath(jarName, ato)).thenCallRealMethod();
            classLoader = mock(AbstractClassLoader.class);
            when(utils.getCurrentThreadContextClassLoader()).thenReturn(classLoader);
            InputStream libPath = utils.getLibPath(jarName, ato);
            Assertions.assertNull(libPath);
            Assertions.assertNotEquals(mockPath, ato.get());
            assertVerify(1, 0, 0);
        }
        @SneakyThrows
        @Test
        void testGetLibPathIsURLClassLoaderButURLsIsEmpty() {
            when(utils.getLibPath(jarName, ato)).thenCallRealMethod();
            classLoader = mock(URLClassLoader.class);
            when(utils.getCurrentThreadContextClassLoader()).thenReturn(classLoader);
            when(((URLClassLoader)classLoader).getURLs()).thenReturn(new URL[]{});
            InputStream libPath = utils.getLibPath(jarName, ato);
            Assertions.assertNull(libPath);
            Assertions.assertNotEquals(mockPath, ato.get());
            assertVerify(1, 0, 0);
        }
        @SneakyThrows
        @Test
        void testGetLibPathIsURLClassLoaderAndURLsNotEmptyButJarPathNotContainsJarName() {
            when(utils.getLibPath(jarName, ato)).thenCallRealMethod();
            classLoader = mock(URLClassLoader.class);
            when(utils.getCurrentThreadContextClassLoader()).thenReturn(classLoader);
            when(((URLClassLoader)classLoader).getURLs()).thenReturn(urls);
            when(url.getPath()).thenReturn("any-path");
            InputStream libPath = utils.getLibPath(jarName, ato);
            Assertions.assertNull(libPath);
            Assertions.assertNotEquals(mockPath, ato.get());
            assertVerify(1, 1, 0);
        }
        @SneakyThrows
        void assertVerify(int classLoaderTimes, int getPathTimes, int openStreamTimes) {
            verify(utils, times(classLoaderTimes)).getCurrentThreadContextClassLoader();
            verify(url, times(getPathTimes)).getPath();
            verify(url, times(openStreamTimes)).openStream();
        }
    }

    @Nested
    class GetPythonConfigTest {
        File file;
        @BeforeEach
        void init() throws IOException {
            file = mock(File.class);
            when(file.exists()).thenReturn(true);
            when(file.isFile()).thenReturn(true);

            when(utils.getPythonConfig(any(File.class))).thenCallRealMethod();
        }

        @Test
        void testGetPythonConfigNormal() {
            assertVerify(file, 1, 1, 1, 1);
        }


        @Test
        void testGetPythonConfigNullConfigFile() {
            when(utils.getPythonConfig(null)).thenCallRealMethod();
            assertVerify(null, 0, 0, 0, 0);
        }
        @Test
        void testGetPythonConfigNotExistsConfigFile() {
            when(file.exists()).thenReturn(false);
            assertVerify(file, 0, 0, 1, 0);
        }
        @Test
        void testGetPythonConfigNotFileConfigFile() {
            when(file.isFile()).thenReturn(false);
            assertVerify(file, 0, 0, 1, 1);
        }
        @Test
        void testGetPythonConfigNotExistsAndNotIsFileConfigFile() {
            when(file.isFile()).thenReturn(false);
            when(file.exists()).thenReturn(false);
            assertVerify(file, 0, 0, 1, 0);
        }
        @Test
        void testGetPythonConfigRreadFileToStringThrowIOException() {
            assertVerify(file, 0, 1, 1, 1, true);
        }

        void assertVerify(File configFileTemp, int fromJsonTimes, int readFileToStringTimes, int existsTimes, int isFileTimes){
            assertVerify(configFileTemp, fromJsonTimes, readFileToStringTimes, existsTimes, isFileTimes, false);
        }

        void assertVerify(File configFileTemp, int fromJsonTimes, int readFileToStringTimes, int existsTimes, int isFileTimes, boolean needThrow) {
            try (MockedStatic<TapSimplify> tapSimplifyMockedStatic = mockStatic(TapSimplify.class);
                 MockedStatic<FileUtils> fileUtilsMockedStatic = mockStatic(FileUtils.class);
            ) {
                tapSimplifyMockedStatic.when(() -> {
                    TapSimplify.fromJson(anyString());
                }).thenReturn(mock(Map.class));

                fileUtilsMockedStatic.when(() -> {
                    FileUtils.readFileToString(any(File.class), any(Charset.class));
                }).then((m) -> {
                    if (needThrow) {
                        throw mock(IOException.class);
                    }
                    return "{}";
                });

                Map<String, Object> pythonConfig = utils.getPythonConfig(configFileTemp);
                Assertions.assertNotNull(pythonConfig);
                tapSimplifyMockedStatic.verify(() -> {TapSimplify.fromJson(anyString());}, times(fromJsonTimes));
                fileUtilsMockedStatic.verify(() -> {FileUtils.readFileToString(any(File.class), any(Charset.class));}, times(readFileToStringTimes));
            } finally {
                verify(file, times(existsTimes)).exists();
                verify(file, times(isFileTimes)).isFile();
            }
        }


//        @Test
//        public void testGetPythonConfig() {
//            File file = new File("temp.json");
//            createFile(file);
//            try {
//                Map<String, Object> pythonConfig = utils.getPythonConfig(file);
//                Assertions.assertEquals(2, pythonConfig.size());
//                Assertions.assertTrue(pythonConfig.containsKey("key1"));
//                Assertions.assertTrue(pythonConfig.containsKey("key2"));
//                Assertions.assertEquals("name", pythonConfig.get("key1"));
//                Assertions.assertEquals("id", pythonConfig.get("key2"));
//            } finally {
//                utils.deleteFile(file, new TapLog());
//            }
//        }
    }

    @Nested
    class ConcatTest {
        @Test
        public void testConcat() {
            when(utils.concat(anyString(), anyString(), anyString())).thenCallRealMethod();
            String path = "tip";
            String p = utils.concat(path, "s", "s1");
            String excepted = path + File.separator + "s" + File.separator + "s1";
            Assertions.assertEquals(excepted, p);
        }
    }

    @Nested
    class DoCopyTest {
        File file;
        File target;

        @SneakyThrows
        @BeforeEach
        void init() {
            file = mock(File.class);
            when(file.getName()).thenReturn("mock-file");
            when(file.exists()).thenReturn(true);
            when(file.isDirectory()).thenReturn(true);
            when(file.isFile()).thenReturn(true);

            target = mock(File.class);
            when(target.getPath()).thenReturn("mock-path");

            doNothing().when(utils).copyFile(any(File.class), any(File.class));
            doNothing().when(utils).copy(any(File.class), any(File.class));

            doCallRealMethod().when(utils).doCopy(any(File.class), any(File.class));
        }

        @SneakyThrows
        void assertVerify(File fileTemp, File targetTemp,
                          int concatTimes,
                          int existsTimes, int isDirectoryTimes,
                          int copyFileTimes,
                          int getPathTimes,
                          int getNameTimes, int isFileTimes,
                          int copyTimes) {
            try(MockedStatic<FilenameUtils> mockedStatic = mockStatic(FilenameUtils.class)) {
                mockedStatic.when(() -> {
                    FilenameUtils.concat(anyString(), anyString());
                }).thenReturn("mock-FilenameUtils-concat");
                utils.doCopy(fileTemp, targetTemp);
                mockedStatic.verify(()->{FilenameUtils.concat(anyString(), anyString());}, times(concatTimes));
            } finally {
                verify(file, times(existsTimes)).exists();
                verify(file, times(isDirectoryTimes)).isDirectory();
                verify(utils, times(copyFileTimes)).copyFile(any(File.class), any(File.class));
                verify(target, times(getPathTimes)).getPath();
                verify(file, times(getNameTimes)).getName();
                verify(file, times(isFileTimes)).isFile();
                verify(utils, times(copyTimes)).copy(any(File.class), any(File.class));
            }
        }

        @SneakyThrows
        @Test
        void testDoCopyNormal() {
            assertVerify(file, target, 1, 1, 1, 1, 1, 1, 0, 0);
        }

        @SneakyThrows
        @Test
        void testDoCopyNullFile() {
            doCallRealMethod().when(utils).doCopy(null, target);
            assertVerify(null, target, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        @Test
        void testDoCopyFileNotExists() {
            when(file.exists()).thenReturn(false);
            assertVerify(file, target, 0, 1, 0, 0, 0, 0, 0, 0);
        }

        @SneakyThrows
        @Test
        void testDoCopyNullTarget() {
            doCallRealMethod().when(utils).doCopy(file, null);
            assertVerify(file, null, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        @Test
        void testDoCopyFileNotDirectory() {
            when(file.isDirectory()).thenReturn(false);
            assertVerify(file, target, 1, 1, 1, 0, 1, 1, 1, 1);
        }

        @SneakyThrows
        @Test
        void testDoCopyFileNotIsFile() {
            when(file.isFile()).thenReturn(false);
            assertVerify(file, target, 1, 1, 1, 1, 1, 1, 0, 0);
        }

        @SneakyThrows
        @Test
        void testDoCopyFileNotDirectoryAndNotIsFile() {
            when(file.isFile()).thenReturn(false);
            when(file.isDirectory()).thenReturn(false);
            assertVerify(file, target, 0, 1, 1, 0, 0, 0, 1, 0);
        }
    }

    @Nested
    class CopyFileTest {
        File file;
        File target;
        File[] files;

        @SneakyThrows
        @BeforeEach
        void init() {
            files = new File[]{mock(File.class)};

            file = mock(File.class);
            when(file.isDirectory()).thenReturn(true);
            when(file.listFiles()).thenReturn(files);

            target = mock(File.class);
            when(target.exists()).thenReturn(true);
            when(target.isDirectory()).thenReturn(true);
            when(target.mkdirs()).thenReturn(true);

            doNothing().when(utils).doCopy(any(File.class), any(File.class));

            doCallRealMethod().when(utils).copyFile(any(File.class), any(File.class));
        }

        void assertVerify(File fileTemp, File targetTemp, int fileIsDirectory, int fileListFiles,
                          int targetExists, int targetIsDirectory, int targetMkdirTimes, int doCopy) throws Exception {
            try {
                utils.copyFile(fileTemp, targetTemp);
            } finally {
                verify(file, times(fileIsDirectory)).isDirectory();
                verify(file, times(fileListFiles)).listFiles();
                verify(target, times(targetExists)).exists();
                verify(target, times(targetIsDirectory)).isDirectory();
                verify(target, times(targetMkdirTimes)).mkdirs();
                verify(utils, times(doCopy)).doCopy(any(File.class), any(File.class));
            }
        }

        @Test
        void testCopyFileNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify(file, target, 1, 1, 1, 1, 0, 1));
        }

        @SneakyThrows
        @Test
        void testCopyFileNullFile() {
            doCallRealMethod().when(utils).copyFile(null, target);
            Assertions.assertDoesNotThrow(() -> assertVerify(null, target, 0, 0, 0, 0, 0, 0));
        }

        @SneakyThrows
        @Test
        void testCopyFileNullTarget() {
            doCallRealMethod().when(utils).copyFile(file, null);
            Assertions.assertDoesNotThrow(() -> assertVerify(file, null, 0, 0, 0, 0, 0, 0));
        }

        @SneakyThrows
        @Test
        void testCopyFileNotFileDirectory() {
            when(file.isDirectory()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> assertVerify(file, target, 1, 0, 1, 1, 0, 1));
        }

        @SneakyThrows
        @Test
        void testCopyFileTargetNotExists() {
            when(target.exists()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> assertVerify(file, target, 1, 1, 1, 0, 1, 1));
        }

        @SneakyThrows
        @Test
        void testCopyFileTargetNotDirectory() {
            when(target.isDirectory()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> assertVerify(file, target, 1, 1, 1, 1, 1, 1));
        }

        @SneakyThrows
        @Test
        void testCopyFileNullFiles() {
            when(file.listFiles()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> assertVerify(file, target, 1, 1, 1, 1, 0, 0));
        }

        @SneakyThrows
        @Test
        void testCopyFileEmptyFiles() {
            when(file.listFiles()).thenReturn(new File[]{});
            Assertions.assertDoesNotThrow(() -> assertVerify(file, target, 1, 1, 1, 1, 0, 0));
        }

        @SneakyThrows
        @Test
        void testCopyFileThrowException() {
            doAnswer(w->{throw mock(Exception.class);}).when(utils).doCopy(any(File.class), any(File.class));
            Assertions.assertThrows(Exception.class, () -> assertVerify(file, target, 1, 1, 1, 1, 0, 1));
        }
    }

    @Nested
    class NeedSkipTest {
        PythonUtils utils;
        @BeforeEach
        void init() {
            utils = new PythonUtils();
        }
        @Test
        void testNeedSkipNormalNormal() {
            File f = mock(File.class);
            when(f.exists()).thenReturn(true);
            when(f.isFile()).thenReturn(true);
            when(f.getName()).thenReturn("setup.py");
            boolean needSkip = utils.needSkip(f);
            Assertions.assertFalse(needSkip);
        }
        @Test
        void testNeedSkipNormalNotExist() {
            File f = mock(File.class);
            when(f.exists()).thenReturn(false);
            when(f.isFile()).thenReturn(true);
            when(f.getName()).thenReturn("setup.py");
            boolean needSkip = utils.needSkip(f);
            Assertions.assertTrue(needSkip);
        }
        @Test
        void testNeedSkipNormalNotFile() {
            File f = mock(File.class);
            when(f.exists()).thenReturn(true);
            when(f.isFile()).thenReturn(false);
            when(f.getName()).thenReturn("setup.py");
            boolean needSkip = utils.needSkip(f);
            Assertions.assertTrue(needSkip);
        }
        @Test
        void testNeedSkipNormalNotName() {
            File f = mock(File.class);
            when(f.exists()).thenReturn(true);
            when(f.isFile()).thenReturn(true);
            when(f.getName()).thenReturn("install.py");
            boolean needSkip = utils.needSkip(f);
            Assertions.assertTrue(needSkip);
        }
        @Test
        void testNeedSkipNormalNullFile() {
            boolean needSkip = utils.needSkip(null);
            Assertions.assertTrue(needSkip);
        }
    }

    @Nested
    class UnzipIeJarTest {
        @Nested
        class NoParams {
            @BeforeEach
            void init() {
                when(utils.unzipIeJar(any(File.class), anyString())).thenReturn(true);
                when(utils.unzipIeJar()).thenCallRealMethod();
            }
            @Test
            void testUnzipIeJarNormal() {
                boolean unZipped = utils.unzipIeJar();
                Assertions.assertTrue(unZipped);
                verify(utils, times(1)).unzipIeJar(any(File.class), anyString());
            }
        }

        @Nested
        class FullParams{
            File agentFile;
            @BeforeEach
            void init() {
                agentFile = mock(File.class);
                when(agentFile.exists()).thenReturn(true);
                when(agentFile.getAbsolutePath()).thenReturn("mock-path");
                when(utils.unzipIeJar(any(File.class), anyString())).thenCallRealMethod();
            }

            @Test
            void testUnzipIeJarNormal() {
                assertVerify(agentFile, false, 1, true, 1, 1);
            }

            @Test
            void testUnzipIeJarNullFile() {
                when(utils.unzipIeJar(null, "mock-path")).thenCallRealMethod();
                assertVerify(null, false, 0, false, 0, 0);
            }

            @Test
            void testUnzipIeJarNotExistsFile() {
                when(agentFile.exists()).thenReturn(false);
                assertVerify(agentFile, false, 0, false, 1, 0);
            }

            @Test
            void testUnzipIeJarThrowWhenUnZip() {
                assertVerify(agentFile, true, 1, false, 1, 1);
            }

            void assertVerify(File tempFile, boolean needThrow, int unzipTimes, boolean value, int existsTimes, int getAbsolutePathTimes) {
                try (MockedStatic<ZipUtils> mockedStatic = mockStatic(ZipUtils.class)) {
                    mockedStatic.when(() -> {
                        ZipUtils.unzip(anyString(), anyString());
                    }).then((m)->{
                        if(!needThrow){
                            return null;
                        }
                        throw mock(IOException.class);
                    });
                    boolean unZipped = utils.unzipIeJar(tempFile, "mock-path");
                    Assertions.assertEquals(value, unZipped);
                    mockedStatic.verify(() -> {ZipUtils.unzip(anyString(), anyString());}, times(unzipTimes));
                } finally {
                    verify(agentFile, times(existsTimes)).exists();
                    verify(agentFile, times(getAbsolutePathTimes)).getAbsolutePath();
                }
            }
        }
    }

    @Nested
    class GetPythonThreadPackageFileTest {
        @Test
        void testGetPythonThreadPackageFile() {
            when(utils.getPythonThreadPackageFile()).thenCallRealMethod();
            File packageFile = utils.getPythonThreadPackageFile();
            Assertions.assertNotNull(packageFile);
            Assertions.assertEquals(PythonUtils.PYTHON_THREAD_PACKAGE_PATH, packageFile.getName());
        }
    }

    @Nested
    class GetAtomicReferenceTest {
        @Test
        void testGetAtomicReferenceNormal() {
            when(utils.getAtomicReference()).thenCallRealMethod();
            AtomicReference<String> reference = utils.getAtomicReference();
            Assertions.assertNotNull(reference);
            Assertions.assertNull(reference.get());
        }
    }

    @Nested
    class ExecuteTest {
        AtomicReference<String> reference;
        InputStream inputStream;
        File pythonThreadPackageFile;

        @SneakyThrows
        @BeforeEach
        void init() {
            reference = mock(AtomicReference.class);
            when(reference.get()).thenReturn("mock-pyJarPath");

            inputStream = mock(InputStream.class);

            pythonThreadPackageFile = mock(File.class);
            when(pythonThreadPackageFile.exists()).thenReturn(true);
            when(pythonThreadPackageFile.mkdirs()).thenReturn(true);

            when(utils.getAtomicReference()).thenReturn(reference);
            when(utils.concat(anyString(), anyString())).thenReturn("mock-concat");
            when(utils.getLibPath(anyString(), any(AtomicReference.class))).thenReturn(inputStream);
            when(utils.getPythonThreadPackageFile()).thenReturn(pythonThreadPackageFile);
            doNothing().when(utils).saveTempZipFile(any(InputStream.class), anyString(), any(Log.class));
            when(utils.setPackagesResources(any(Log.class), anyString(), anyString(), anyString(), anyString())).thenReturn(1);

            when(utils.execute(anyString(), anyString(), any(Log.class))).thenCallRealMethod();
        }

        @SneakyThrows
        void assertVerify(Integer executeResult, int referenceGetTimes, int getPythonThreadPackageFileTimes,
                          int existsTimes, int mkdirsTimes, int concatTimes,
                          int saveTempZipFileTimes, int setPackagesResourcesTimes,
                          int warnTimes) {
            Integer execute = utils.execute("x", "y", log);
            Assertions.assertNotNull(execute);
            Assertions.assertEquals(executeResult, execute);
            verify(utils, times(1)).getAtomicReference();
            verify(utils, times(1)).getLibPath(anyString(), any(AtomicReference.class));
            verify(reference, times(referenceGetTimes)).get();
            verify(utils, times(getPythonThreadPackageFileTimes)).getPythonThreadPackageFile();
            verify(pythonThreadPackageFile, times(existsTimes)).exists();
            verify(pythonThreadPackageFile, times(mkdirsTimes)).mkdirs();
            verify(utils, times(concatTimes)).concat(anyString(), anyString());
            verify(utils, times(saveTempZipFileTimes)).saveTempZipFile(any(InputStream.class), anyString(), any(Log.class));
            verify(utils, times(setPackagesResourcesTimes)).setPackagesResources(any(Log.class), anyString(), anyString(), anyString(), anyString());
            verify(log, times(warnTimes)).warn(anyString());
        }
        @Test
        void testExecuteNormal() {
            assertVerify(1, 1, 1, 1, 0, 1, 1, 1, 0);
        }

        @SneakyThrows
        @Test
        void testExecuteNullInputStream() {
            when(utils.getLibPath(anyString(), any(AtomicReference.class))).thenReturn(null);
            assertVerify(-1, 1, 0, 0, 0, 0, 0, 0, 0);
        }

        @Test
        void testExecuteNullPyJarPath() {
            when(reference.get()).thenReturn(null);
            assertVerify(-1, 1, 0, 0, 0, 0, 0, 0, 0);
        }

        @SneakyThrows
        @Test
        void testExecuteNullPyJarPathAndNullInputStream() {
            when(utils.getLibPath(anyString(), any(AtomicReference.class))).thenReturn(null);
            when(reference.get()).thenReturn(null);
            assertVerify(-1, 1, 0, 0, 0, 0, 0, 0, 0);
        }

        @Test
        void testExecutePythonThreadPackageFileNotExists() {
            when(pythonThreadPackageFile.exists()).thenReturn(false);
            assertVerify(1, 1, 1, 1, 1, 1, 1, 1, 0);
        }

        @SneakyThrows
        @Test
        void testExecuteGetLibPathThrowIOException() {
            when(utils.getLibPath(anyString(), any(AtomicReference.class))).then(w -> {
                IOException mock = mock(IOException.class);
                when(mock.getMessage()).thenReturn("mock-IOException");
                throw mock;
            });
            assertVerify(0, 0, 0, 0, 0, 0, 0, 0, 1);
        }
    }

    @Nested
    class SaveTempFileTest {
        InputStream inputStream;
        String savePath;
        FileOutputStream outputStream;
        RandomAccessFile file;

        int [] readResults;
        int index;

        @SneakyThrows
        @BeforeEach
        void init() {
            readResults = new int[]{100};
            index = 0;

            inputStream = mock(FileInputStream.class);
            when(inputStream.read(any(byte[].class))).then(w -> {
                if (index < readResults.length) {
                    return readResults[index++];
                } else {
                    return  -1;
                }
            });
            doNothing().when(inputStream).close();

            savePath = "mock-path";

            outputStream = mock(FileOutputStream.class);
            doNothing().when(outputStream).write(any(byte[].class), anyInt(), anyInt());
            doNothing().when(outputStream).close();

            file = mock(RandomAccessFile.class);
            doNothing().when(file).close();

            when(utils.getFileOutputStream(anyString())).thenReturn(outputStream);
            when(utils.getRandomAccessFile(any(File.class))).thenReturn(file);

            doCallRealMethod().when(utils).saveTempZipFile(any(InputStream.class), anyString(), any(Log.class));
        }

        void assertVerify(InputStream inputStreamTemp,
                          int getFileOutputStream, int getRandomAccessFile,
                          int fileClose,
                          int read, int write,
                          int logTimes,
                          int inClose, int outClose) throws IOException {
            try {
                utils.saveTempZipFile(inputStreamTemp, savePath, log);
            } finally {
                verify(utils, times(getFileOutputStream)).getFileOutputStream(anyString());
                verify(utils, times(getRandomAccessFile)).getRandomAccessFile(any(File.class));
                verify(file, times(fileClose)).close();
                verify(inputStream, times(read)).read(any(byte[].class));
                verify(outputStream, times(write)).write(any(byte[].class), anyInt(), anyInt());
                verify(log, times(logTimes)).info(anyString());
                verify(inputStream, times(inClose)).close();
                verify(outputStream, times(outClose)).close();
            }
        }

        @Test
        void testSaveTempFileNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify(inputStream, 1, 1, 1, 2, 1, 0, 1, 1));
        }

        @Test
        void testSaveTempFileNullInputStream() {
            doCallRealMethod().when(utils).saveTempZipFile(null, savePath, log);
            Assertions.assertDoesNotThrow(() -> assertVerify(null, 0, 0, 0, 0, 0, 0, 0, 0));
        }

        @SneakyThrows
        @Test
        void testReadWithIOException() {
            when(inputStream.read(any(byte[].class))).then(w-> {throw new IOException("error");});
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(inputStream, 1, 1, 1, 1, 0, 1, 1, 1);
            });
        }

        @SneakyThrows
        @Test
        void testWriteWithIOException() {
            doAnswer(w -> {
                throw new IOException("error");
            }).when(outputStream).write(any(byte[].class), anyInt(), anyInt());
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(inputStream, 1, 1, 1, 1, 1, 1, 1, 1);
            });
        }

        @SneakyThrows
        @Test
        void testGetFileOutputStreamWithFileNotFundException() {
            when(utils.getFileOutputStream(anyString())).then(w->{
                throw new FileNotFoundException("error");
            });
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(inputStream, 1, 0, 0, 0, 0, 1, 1, 0);
            });
        }

        @SneakyThrows
        @Test
        void testGetRandomAccessFileWithFileNotFundException() {
            when(utils.getRandomAccessFile(any(File.class))).then(w->{
                throw new FileNotFoundException("error");
            });
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(inputStream, 1, 1, 0, 0, 0, 1, 1, 1);
            });
        }

        @SneakyThrows
        @Test
        void testInputStreamCloseWithIOException() {
            doAnswer(w -> {
                throw new IOException("error");
            }).when(inputStream).close();
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(inputStream, 1, 1, 1, 2, 1, 1, 1, 1);
            });
        }

        @SneakyThrows
        @Test
        void testRandomAccessFileCloseWithIOException() {
            doAnswer(w -> {
                throw new IOException("error");
            }).when(outputStream).close();
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(inputStream, 1, 1, 1, 2, 1, 1, 1, 1);
            });
        }
    }

    @Nested
    class CopyTest {
        File from;
        File to;
        String fromPath;
        String toPath;
        FileInputStream inputStream;
        FileOutputStream outputStream;
        int [] readResults;
        int index;


        @BeforeEach
        void init () throws Exception {
            fromPath = "from";
            toPath = "to";
            index = 0;

            from = mock(File.class);
            when(from.getAbsolutePath()).thenReturn(fromPath);
            to = mock(File.class);
            when(to.getAbsolutePath()).thenReturn(toPath);

            readResults = new int[]{100};
            inputStream = mock(FileInputStream.class);
            when(inputStream.read(any(byte[].class))).then(w -> {
                if (index < readResults.length) {
                    return readResults[index++];
                } else {
                    return  -1;
                }
            });
            doNothing().when(inputStream).close();

            outputStream = mock(FileOutputStream.class);
            doNothing().when(outputStream).write(any(byte[].class), anyInt(), anyInt());
            doNothing().when(outputStream).close();

            when(utils.getFileInputStream(fromPath)).thenReturn(inputStream);
            when(utils.getFileOutputStream(toPath)).thenReturn(outputStream);
        }

        void assertVerify(File fromTemp, File toTemp,
                          int newFileInputStream, int newFileOutputStream,
                          int read, int write,
                          int inClose, int outClose) throws IOException {
            try {
                utils.copy(fromTemp, toTemp);
            } finally {
                verify(utils, times(newFileInputStream)).getFileInputStream(anyString());
                verify(utils, times(newFileOutputStream)).getFileOutputStream(anyString());
                verify(inputStream, times(read)).read(any(byte[].class));
                verify(outputStream, times(write)).write(any(byte[].class), anyInt(), anyInt());
                verify(inputStream, times(inClose)).close();
                verify(outputStream, times(outClose)).close();
            }
        }

        @SneakyThrows
        @Test
        void testCopyNormal() {
            doCallRealMethod().when(utils).copy(from, to);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(from, to, 1, 1, 2, 1, 1, 1);
            });
        }

        @SneakyThrows
        @Test
        void testCopyNullFromFile() {
            doCallRealMethod().when(utils).copy(null, to);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(null, to, 0, 0, 0, 0, 0, 0);
            });
        }

        @SneakyThrows
        @Test
        void testCopyNullToFile() {
            doCallRealMethod().when(utils).copy(from, null);
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(from, null, 0, 0, 0, 0, 0, 0);
            });
        }

        @SneakyThrows
        @Test
        void testCopyNullFromFileNotFundException() {
            doCallRealMethod().when(utils).copy(from, to);
            when(utils.getFileInputStream(anyString())).then(w->{
                throw new FileNotFoundException();
            });
            Assertions.assertThrows(FileNotFoundException.class, () -> {
                assertVerify(from, to, 1, 0, 0, 0, 0, 0);
            });
        }

        @SneakyThrows
        @Test
        void testCopyNullToFileNotFundException() {
            doCallRealMethod().when(utils).copy(from, to);
            when(utils.getFileOutputStream(anyString())).then(w->{
                throw new FileNotFoundException();
            });
            Assertions.assertThrows(FileNotFoundException.class, () -> {
                assertVerify(from, to, 1, 1, 0, 0, 1, 0);
            });
        }

        @SneakyThrows
        @Test
        void testCopyReadIOException() {
            doCallRealMethod().when(utils).copy(from, to);
            when(inputStream.read(any(byte[].class))).then(w-> {throw new IOException();});
            when(utils.getFileInputStream(anyString())).thenReturn(inputStream);
            Assertions.assertThrows(IOException.class, () -> {
                assertVerify(from, to, 1, 1, 1, 0, 1, 1);
            });
        }
        @SneakyThrows
        @Test
        void testCopyWriteIOException() {
            doCallRealMethod().when(utils).copy(from, to);
            doAnswer(w -> {
                throw new IOException();
            }).when(outputStream).write(any(byte[].class), anyInt(), anyInt());
            Assertions.assertThrows(IOException.class, () -> {
                assertVerify(from, to, 1, 1, 1, 1, 1, 1);
            });
        }

    }

    @Nested
    class UnZipAsCacheFileTest {
        File file;
        @BeforeEach
        void init(){
            file = mock(File.class);
            when(file.getAbsolutePath()).thenReturn("mock-path");

            doCallRealMethod().when(utils).unZipAsCacheFile(any(File.class), anyString(), anyString(), any(Log.class));
        }
        @Test
        void testUnZipAsCacheFileNormal() {
            assertVerify(true, 2, 0, w -> null);
        }

        @Test
        void testUnZipAsCacheFileWithException(){
            assertVerify(false, 1, 1, a -> {throw new Exception("mock-fail");});
        }

        void assertVerify(boolean value, int normalLogTimes, int infoTimes, Answer<?> unzipFunction) {
            try(MockedStatic<ZipUtils> mockedStatic = mockStatic(ZipUtils.class)){
                mockedStatic.when(()->ZipUtils.unzip(anyString(), anyString())).thenAnswer(unzipFunction);
                boolean cacheFile = utils.unZipAsCacheFile(file, "a", "b", log);
                mockedStatic.verify(()->ZipUtils.unzip(anyString(), anyString()), times(1));
                Assertions.assertEquals(value, cacheFile);
            } finally {
                verify(log, times(normalLogTimes)).info(anyString(), anyString());
                verify(log, times(infoTimes)).info(anyString(), anyString(), anyString());
            }
        }
    }

    @Nested
    class BeforeCopyLibFilesTest {
        @SneakyThrows
        @BeforeEach
        void init() {
            doNothing().when(utils).copyFile(any(File.class), any(File.class));
            when(utils.concat(anyString(), anyString(), anyString())).thenReturn("mock-path.jar");
            when(utils.beforeCopyLibFiles(anyString())).thenCallRealMethod();
        }
        @SneakyThrows
        @Test
        void testBeforeCopyLibFilesNormal() {
            Assertions.assertDoesNotThrow(()->{
                File file = utils.beforeCopyLibFiles("mock");
                Assertions.assertNotNull(file);
                Assertions.assertEquals("mock-path.jar", file.getName());
            });
            verify(utils, times(1)).copyFile(any(File.class), any(File.class));
            verify(utils, times(1)).concat(anyString(), anyString(), anyString());
        }
        @SneakyThrows
        @Test
        void testBeforeCopyLibFilesThrowException() {
            doAnswer((w)->{throw new Exception("fail");}).when(utils).copyFile(any(File.class), any(File.class));
            Assertions.assertThrows(Exception.class, ()-> utils.beforeCopyLibFiles("mock"));
            verify(utils, times(1)).copyFile(any(File.class), any(File.class));
            verify(utils, times(0)).concat(anyString(), anyString(), anyString());
        }
    }

    @Nested
    class CopyLibFilesTest {
        File file;
        @SneakyThrows
        @BeforeEach
        void init() {
            file = mock(File.class);
            when(file.getAbsolutePath()).thenReturn("mock-path");
            when(file.exists()).thenReturn(true);
            when(file.mkdirs()).thenReturn(true);

            when(utils.beforeCopyLibFiles(anyString())).thenReturn(file);
            doCallRealMethod().when(utils).copyLibFiles(anyString(), anyString(), anyString(), any(Log.class));
        }

        @Test
        void testCopyLibFilesNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify(w->null, 1, 1, 1, 0, 1, 0, 1));
        }
        @Test
        void testCopyLibFilesFileNotExists() {
            when(file.exists()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> assertVerify(w->null, 1, 1, 1, 1, 1, 1, 1));
        }
        @SneakyThrows
        @Test
        void testCopyLibFilesFileThrowExceptionOnBeforeCopyLibFiles() {
            when(utils.beforeCopyLibFiles(anyString())).thenAnswer(w->{throw new Exception("x");});
            Assertions.assertThrows(Exception.class, () -> assertVerify(w->null, 0, 1, 0, 0, 0, 0, 0));
        }
        @Test
        void testCopyLibFilesFileThrowExceptionOnCopyToDirectory() {
            Assertions.assertThrows(IOException.class, () -> assertVerify(w->{throw new IOException("x");}, 1, 1, 1, 0, 1, 0, 1));
        }

        @SneakyThrows
        void assertVerify(Answer<?> copyToDirectory, int copyToDirectoryTimes,
                          int beforeCopyLibFiles, int exists, int mkdirs,
                          int info2, int info3, int info4) {
            try(MockedStatic<FileUtils> staticMethod = mockStatic(FileUtils.class)) {
                staticMethod.when(()->FileUtils.copyToDirectory(any(File.class), any(File.class))).thenAnswer(copyToDirectory);
                utils.copyLibFiles("a", "b", "c", log);
                staticMethod.verify(()->FileUtils.copyToDirectory(any(File.class), any(File.class)), times(copyToDirectoryTimes));
            } finally {
                verify(utils, times(beforeCopyLibFiles)).beforeCopyLibFiles(anyString());
                verify(file, times(exists)).exists();
                verify(file, times(mkdirs)).mkdirs();
                verify(file, times(copyToDirectoryTimes)).getAbsolutePath();
                verify(log, times(info2)).info(anyString(), anyString());
                verify(log, times(info3)).info(anyString(), anyString(), anyBoolean());
                verify(log, times(info4)).info(anyString(), anyString(), anyString(), anyString());
            }
        }
    }

    @Nested
    class MvJarLibsToLibCachePathTest {
        File jarLib;
        File toPath;
        File file3;
        File file4;
        File needDelete;
        String libPathName;
        @SneakyThrows
        @BeforeEach
        void init() {
            libPathName = "mock-libPathName";
            jarLib = mock(File.class);
            toPath = mock(File.class);
            when(toPath.exists()).thenReturn(false);
            when(toPath.mkdirs()).thenReturn(true);

            file3 = mock(File.class);
            file4 = mock(File.class);
            needDelete = mock(File.class);
            when(needDelete.exists()).thenReturn(true);

            when(utils.concatToFile(libPathName, PythonUtils.LIB_U_TAG)).thenReturn(jarLib);
            when(utils.concatToFile(PythonUtils.PYTHON_THREAD_PACKAGE_PATH, PythonUtils.LIB_U_TAG, PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(toPath);
            when(utils.concatToFile(libPathName, PythonUtils.PYTHON_SITE_PACKAGES_VERSION_CONFIG)).thenReturn(file3);
            when(utils.concatToFile(PythonUtils.PYTHON_THREAD_PACKAGE_PATH)).thenReturn(file4);
            when(utils.concatToFile(PythonUtils.PYTHON_THREAD_PACKAGE_PATH, PythonUtils.LIB_U_TAG, "item-packages", PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH)).thenReturn(needDelete);

            doNothing().when(utils).copyFile(jarLib, toPath);
            doNothing().when(utils).copyFile(file3, file4);
            doCallRealMethod().when(utils).mvJarLibsToLibCachePath(anyString(), anyString(), any(Log.class));
        }

        @Test
        void testMvJarLibsToLibCachePathNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  w -> null));
        }

        @Test
        void testMvJarLibsToLibCachePathToPathFileExists() {
            when(toPath.exists()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,  w -> null));
        }

        @Test
        void testMvJarLibsToLibCachePathToPathCanNotMkDirs() {
            when(toPath.mkdirs()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0,  w -> null));
        }

        @Test
        void testMvJarLibsToLibCachePathNeedDeleteNotExists() {
            when(needDelete.exists()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0,  w -> null));
        }

        void assertVerify(int jarLibTimes, int toPathTimes, int needDeleteTimes,
                          int toPathExistsTimes, int toPathMkDirsTimes,
                          int copyFile1Times,
                          int file3Times, int file4Times,
                          int copyFile2Times,
                          int needDeleteExistsTimes,
                          int logTimes,
                          int deleteDirectory, Answer<?> answer) throws Exception {
            try (MockedStatic<FileUtils> mockedStatic = mockStatic(FileUtils.class)) {
                mockedStatic.when(() ->  FileUtils.deleteDirectory(any(File.class))).then(answer);
                utils.mvJarLibsToLibCachePath("mock-str", libPathName, log);
                mockedStatic.verify(() ->  FileUtils.deleteDirectory(any(File.class)), times(deleteDirectory));
            } finally {
                verify(utils, times(jarLibTimes)).concatToFile(libPathName, PythonUtils.LIB_U_TAG);
                verify(utils, times(toPathTimes)).concatToFile(PythonUtils.PYTHON_THREAD_PACKAGE_PATH, PythonUtils.LIB_U_TAG, PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH);

                verify(toPath, times(toPathExistsTimes)).exists();
                verify(toPath, times(toPathMkDirsTimes)).mkdirs();

                verify(utils, times(copyFile1Times)).copyFile(jarLib, toPath);

                verify(utils, times(file3Times)).concatToFile(libPathName, PythonUtils.PYTHON_SITE_PACKAGES_VERSION_CONFIG);
                verify(utils, times(file4Times)).concatToFile(PythonUtils.PYTHON_THREAD_PACKAGE_PATH);

                verify(utils, times(copyFile2Times)).copyFile(file3, file4);

                verify(utils, times(needDeleteTimes)).concatToFile(PythonUtils.PYTHON_THREAD_PACKAGE_PATH, PythonUtils.LIB_U_TAG, "item-packages", PythonUtils.PYTHON_THREAD_SITE_PACKAGES_PATH);
                verify(needDelete, times(needDeleteExistsTimes)).exists();
                verify(log, times(logTimes)).info(anyString(), anyString(), anyString());
            }
        }
    }

    @Nested
    class SetPackagesResourcesTest {
        String zipFileTempPath;
        String unzipPath;
        String pyJarPath;
        String jarName;
        File packagesFile;
        @SneakyThrows
        @BeforeEach
        void init() {
            zipFileTempPath = "zipFileTempPath";
            unzipPath = "unzipPath";
            pyJarPath = "pyJarPath"; //*
            jarName = "jarName"; //&
            packagesFile = mock(File.class);

            when(utils.unZipAsCacheFile(any(File.class), anyString(), anyString(), any(Log.class))).thenReturn(true);
            doNothing().when(utils).copyLibFiles(anyString(), anyString(), anyString(), any(Log.class));
            doNothing().when(utils).mvJarLibsToLibCachePath(anyString(), anyString(), any(Log.class));
            doNothing().when(utils).cleanCache(anyString(), any(Log.class));
            doNothing().when(utils).loopPackagesList(any(File.class), any(Log.class), anyString(), anyString());
            when(utils.getLoopPackagesFile()).thenReturn(packagesFile);
            when(utils.concat(anyString(), anyString())).thenReturn("mock-String");

            when(utils.setPackagesResources(any(Log.class), anyString(), anyString(), anyString(), anyString())).thenCallRealMethod();
        }

        @Test
        void testSetPackagesResourcesNormal() {
            assertVerify(1, 1, 1, 1, 1, 1, 1, 1, 0);
        }

        @Test
        void testSetPackagesResourcesNotUnZipAsCacheFile() {
            when(utils.unZipAsCacheFile(any(File.class), anyString(), anyString(), any(Log.class))).thenReturn(false);
            assertVerify(-3, 1, 0, 0, 0, 0, 0, 0, 0);
        }

        @SneakyThrows
        @Test
        void testSetPackagesResourcesThrowExceptionByCopyLibFilesPyJarPathEndsWithJarName() {
            pyJarPath = "pyJarPath-jarName"; //*
            doAnswer(w->{throw new Exception("fail");}).when(utils).copyLibFiles(anyString(), anyString(), anyString(), any(Log.class));
            assertVerify(-1, 1, 1, 0, 1, 0, 0, 0, 1);
        }

        @SneakyThrows
        @Test
        void testSetPackagesResourcesThrowExceptionByCopyLibFilesPyJarPathNotEndsWithJarName() {
            doAnswer(w->{throw new Exception("fail");}).when(utils).copyLibFiles(anyString(), anyString(), anyString(), any(Log.class));
            assertVerify(-1, 1, 1, 0, 1, 0, 0, 0, 1);
        }

        @SneakyThrows
        @Test
        void testSetPackagesResourcesThrowExceptionByMvJarLibsToLibCachePathPyJarPathEndsWithJarName() {
            pyJarPath = "pyJarPath-jarName"; //*
            doAnswer(w->{throw new Exception("fail");}).when(utils).mvJarLibsToLibCachePath(anyString(), anyString(), any(Log.class));
            assertVerify(-1, 1, 1, 1, 1, 0, 0, 0, 1);
        }

        @SneakyThrows
        @Test
        void testSetPackagesResourcesThrowExceptionByMvJarLibsToLibCachePyJarPathNotEndsWithJarName() {
            doAnswer(w->{throw new Exception("fail");}).when(utils).mvJarLibsToLibCachePath(anyString(), anyString(), any(Log.class));
            assertVerify(-1, 1, 1, 1, 1, 0, 0, 0, 1);
        }

        @SneakyThrows
        void assertVerify(int resultValue,
                          int unZipAsCacheFileTimes, int copyLibFilesTimes,
                          int mvJarLibsToLibCachePathTimes, int cleanCacheTimes,
                          int loopPackagesListTimes, int getLoopPackagesFileTimes, int concatTimes, int logTimes) {
            int result = utils.setPackagesResources(log, zipFileTempPath, unzipPath, pyJarPath, jarName);
            Assertions.assertEquals(resultValue, result);
            verify(utils, times(unZipAsCacheFileTimes)).unZipAsCacheFile(any(File.class), anyString(), anyString(), any(Log.class));
            verify(utils, times(copyLibFilesTimes)).copyLibFiles(anyString(), anyString(), anyString(), any(Log.class));
            verify(utils, times(mvJarLibsToLibCachePathTimes)).mvJarLibsToLibCachePath(anyString(), anyString(), any(Log.class));
            verify(utils, times(cleanCacheTimes)).cleanCache(anyString(), any(Log.class));
            verify(utils, times(loopPackagesListTimes)).loopPackagesList(any(File.class), any(Log.class), anyString(), anyString());
            verify(utils, times(getLoopPackagesFileTimes)).getLoopPackagesFile();
            verify(utils, times(concatTimes)).concat(anyString(), anyString());

            verify(log, times(logTimes)).info(anyString(), anyString(), anyString(), anyString(), anyString());
            verify(log, times(logTimes)).warn(anyString(), anyString());
        }
    }

    @Nested
    class CleanCacheTest {
        File temp1;
        @BeforeEach
        void init() {
            temp1 = mock(File.class);
            when(temp1.exists()).thenReturn(true);

            when(utils.concatToFile(anyString())).thenReturn(temp1);
            doCallRealMethod().when(utils).cleanCache(anyString(), any(Log.class));
        }

        @Test
        void testCleanCacheNormal() {
            assertVerify(1, 1, 1, 1, w-> true, 1);
        }


        @Test
        void testCleanCacheFileNotExists() {
            when(temp1.exists()).thenReturn(false);
            assertVerify(1, 1, 0, 0, w-> true, 0);
        }

        void assertVerify(int concatToFile, int existsTimes, int info1, int info2, Answer<?> answer, int deleteTimes) {
            try (MockedStatic<FileUtils> mockedStatic = mockStatic(FileUtils.class)) {
                mockedStatic.when(() ->  FileUtils.deleteQuietly(any(File.class))).then(answer);
                utils.cleanCache("any-string", log);
                mockedStatic.verify(() ->  FileUtils.deleteQuietly(any(File.class)), times(deleteTimes));
            } finally {
                verify(utils, times(concatToFile)).concatToFile(anyString());
                verify(temp1, times(existsTimes)).exists();
                verify(log, times(info1)).info(anyString(), anyString());
                verify(log, times(info2)).info(anyString());
            }
        }
    }

    @Nested
    class ConcatToFileTest {
        @BeforeEach
        void init() {
            when(utils.concatToFile(anyString(), any(String[].class))).thenCallRealMethod();
            when(utils.concatToFile(anyString())).thenCallRealMethod();
        }

        @Test
        void testConcatToFileNormal() {
            File file = utils.concatToFile("mock-path", new String[]{"sub-path"});
            Assertions.assertNotNull(file);
        }

        @Test
        void testConcatToFileNull() {
            File file = utils.concatToFile("mock-path");
            Assertions.assertNotNull(file);
        }

        @Test
        void testConcatToFileEmpty() {
            File file = utils.concatToFile("mock-path", new String[]{});
            Assertions.assertNotNull(file);
        }
    }

    @Nested
    class PrintMsgTest {
        BufferedReader reader;

        @BeforeEach
        void init() throws IOException {
            reader = mock(BufferedReader.class);
        }

        @Nested
        class PrintMsgWithBufferedReaderTest {
            Exception e;

            AtomicInteger index = new AtomicInteger(0);
            String[] lines;

            @BeforeEach
            void init() throws IOException {
                e = new Exception("error");
                lines = new String[]{"", ""};

                when(reader.readLine()).thenAnswer(w->{
                    if (index.get() < lines.length) {
                        String line = lines[index.get()];
                        index.incrementAndGet();
                        return line;
                    }
                    return null;
                });

                doCallRealMethod().when(utils).printMsg(reader, log);
            }

            void verifyAssert(int linesTimes, int infoTimes, int warnTimes) throws IOException {
                utils.printMsg(reader, log);
                verify(reader, times(linesTimes)).readLine();
                verify(log, times(infoTimes)).info(anyString());
                verify(log, times(warnTimes)).warn(anyString());
            }

            @Test
            void testNormal() {
                try {
                    Assertions.assertDoesNotThrow(() -> {
                        verifyAssert(3, 2, 0);
                    });
                } finally {
                    index.set(0);
                }
            }

            @Test
            void testThrowException() throws IOException {
                try {
                    when(reader.readLine()).thenAnswer(w-> {throw e;});
                    Assertions.assertDoesNotThrow(() -> {
                        verifyAssert(1, 0, 1);
                    });
                } finally {
                    index.set(0);
                }
            }

            @Test
            void testNullBufferedReader() throws IOException {
                BufferedReader temp = null;
                doCallRealMethod().when(utils).printMsg(temp, log);
                utils.printMsg(temp, log);
                verify(reader, times(0)).readLine();
                verify(log, times(0)).info(anyString());
                verify(log, times(0)).warn(anyString());
            }
        }

        @Nested
        class PrintMsgWithInputStreamTest {
            InputStream stream;
            @BeforeEach
            void init() throws IOException {
                stream = mock(InputStream.class);

                when(utils.getBufferedReader(stream)).thenReturn(reader);
                doNothing().when(utils).printMsg(reader, log);
                doCallRealMethod().when(utils).printMsg(stream, log);
            }

            void assertVerify(int getBufferedReaderTimes, int printMsgTimes, int logTimes) {
                utils.printMsg(stream, log);
                verify(utils, times(getBufferedReaderTimes)).getBufferedReader(stream);
                verify(utils, times(printMsgTimes)).printMsg(reader, log);
                verify(log, times(logTimes)).warn(anyString());
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> {
                    assertVerify(1, 1, 0);
                });
            }

            @Test
            void testException() {
                when(utils.getBufferedReader(stream)).thenAnswer(w->{throw new Exception("");});
                Assertions.assertDoesNotThrow(() -> {
                    assertVerify(1, 0, 1);
                });
            }

            @Test
            void testNullInputStream() {
                InputStream temp = null;
                doCallRealMethod().when(utils).printMsg(temp, log);
                Assertions.assertDoesNotThrow(() -> {
                    utils.printMsg(temp, log);
                    verify(utils, times(0)).getBufferedReader(stream);
                    verify(utils, times(0)).printMsg(stream, log);
                    verify(log, times(0)).warn(anyString());
                });
            }
        }

    }
}
