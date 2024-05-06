package com.tapdata.tm.init;

import cn.hutool.core.io.FileUtil;
import com.tapdata.tm.init.scanners.JavaPatchScanner;
import com.tapdata.tm.init.scanners.ScriptPatchScanner;
import com.tapdata.tm.verison.service.VersionService;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.ApplicationArguments;

import java.util.*;

import static org.mockito.Mockito.*;

/**
 * 补丁单测
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/25 16:33 Create
 */
class PatchesRunnerTest {

    PatchesRunner spyPatchesRunner;
    VersionService versionService;
    String mongodbUri;
    ApplicationArguments applicationArguments;

    @BeforeEach
    void setUp() {
        spyPatchesRunner = spy(new PatchesRunner());
        versionService = mock(VersionService.class);
        UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "versionService", versionService);
    }

    @Nested
    class AppType {
        @Test
        void testDAAS() {
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "mongodbUri", mongodbUri);
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "productList", Collections.singletonList("idass"));
            doReturn(Collections.emptyList()).when(spyPatchesRunner).scanPatches(any(), any(), any(), any(), any());

            try (MockedStatic<InitLogMap> initLogMapMockedStatic = mockStatic(InitLogMap.class)) {
                initLogMapMockedStatic.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
                initLogMapMockedStatic.verify(() -> InitLogMap.complete(any()), times(1));
            }
        }

        @Test
        void testDFS() {
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "mongodbUri", mongodbUri);
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "productList", Arrays.asList("idass", "dfs"));
            doReturn(Collections.emptyList()).when(spyPatchesRunner).scanPatches(any(), any(), any(), any(), any());

            try (MockedStatic<InitLogMap> initLogMapMockedStatic = mockStatic(InitLogMap.class)) {
                initLogMapMockedStatic.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
                initLogMapMockedStatic.verify(() -> InitLogMap.complete(any()), times(1));
            }
        }

//        @Test
//        void testDRS() {
//            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "mongodbUri", mongodbUri);
//            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "productList", Arrays.asList("idass", "drs"));
//            doReturn(Collections.emptyList()).when(spyPatchesRunner).scanPatches(any(), any(), any(), any(), any());
//
//            Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
//        }
    }

    @Nested
    class ScanPatchesTest {
        PatchVersion appVersion;
        PatchVersion softVersion;
        Map<String, String> allVariables;

        @BeforeEach
        void setUp() {
            appVersion = new PatchVersion(3, 5, 11);
            softVersion = new PatchVersion(3, 5, 12);
            allVariables = new HashMap<>();
        }

        @Test
        void testLoadScriptFailed() {
            io.tapdata.utils.AppType appType = io.tapdata.utils.AppType.DRS;
            ScriptPatchScanner scriptPatchScanner = new ScriptPatchScanner(appType, allVariables);
            JavaPatchScanner javaScrPatchScanner = new JavaPatchScanner(appType);

            try (MockedStatic<PatchesRunner> patchesRunnerMockedStatic = Mockito.mockStatic(PatchesRunner.class)) {
                patchesRunnerMockedStatic.when(() -> PatchesRunner.patchDir(appType)).thenReturn(null);
                Assertions.assertThrows(RuntimeException.class, () -> spyPatchesRunner.scanPatches(appType, appVersion, softVersion, scriptPatchScanner, javaScrPatchScanner));
            }
        }

        @Test
        void testSkipFileNullVersion() {
            io.tapdata.utils.AppType appType = io.tapdata.utils.AppType.DFS;
            ScriptPatchScanner scriptPatchScanner = new ScriptPatchScanner(appType, allVariables);

            try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
                fileUtilMockedStatic.when(() -> FileUtil.mainName(Mockito.anyString())).thenReturn(null); // set all file name is null
                List<IPatch> allPatch = spyPatchesRunner.scanPatches(appType, appVersion, softVersion, scriptPatchScanner);
                Assertions.assertTrue(allPatch.isEmpty());
            }
        }

        @Test
        void testPatchVersionBetweenAppAndSoft() {
            io.tapdata.utils.AppType appType = io.tapdata.utils.AppType.DAAS;
            ScriptPatchScanner scriptPatchScanner = new ScriptPatchScanner(appType, allVariables);
            JavaPatchScanner javaScrPatchScanner = new JavaPatchScanner(appType);

            List<IPatch> allPatch = spyPatchesRunner.scanPatches(appType, appVersion, softVersion, scriptPatchScanner, javaScrPatchScanner);
            for (IPatch patch : allPatch) {
                Assertions.assertNotNull(patch);
                Assertions.assertTrue(patch.version().compareVersion(appVersion) >= 0);
                Assertions.assertTrue(patch.version().compareVersion(softVersion) <= 0);
            }
        }
    }
}
