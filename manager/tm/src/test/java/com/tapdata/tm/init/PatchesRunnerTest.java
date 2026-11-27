package com.tapdata.tm.init;

import cn.hutool.core.io.FileUtil;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.JsonFilePatch;
import com.tapdata.tm.init.scanners.JavaPatchScanner;
import com.tapdata.tm.init.scanners.ScriptPatchScanner;
import com.tapdata.tm.utils.TMStartMsgUtil;
import com.tapdata.tm.verison.dto.VersionDto;
import com.tapdata.tm.verison.service.VersionService;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.ApplicationArguments;
import org.springframework.data.mongodb.core.query.Query;

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

    @Nested
    class TrackedExecutionTest {

        private PatchType daasScriptType;

        @BeforeEach
        void initTrackedTest() {
            daasScriptType = new PatchType(io.tapdata.utils.AppType.DAAS, PatchTypeEnums.Script);
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "mongodbUri", "mongodb://localhost:27017/unused");
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "productList", Collections.singletonList("idaas"));
        }

        @Test
        void testGroupVersionWatermarkAndExecutedPatchTracking() {
            VersionDto existing = new VersionDto(daasScriptType.toString(), "4.25-3");
            when(versionService.findOne(any(Query.class))).thenReturn(existing);

            List<String> executedOrder = new ArrayList<>();
            IPatch p4254 = dummyPatch(daasScriptType, "4.25-4", "script:4.25-4.json", () -> executedOrder.add("4.25-4:script"));
            IPatch p4255a = dummyPatch(daasScriptType, "4.25-5", "java:PatchA", () -> executedOrder.add("4.25-5:A"));
            IPatch p4255b = dummyPatch(daasScriptType, "4.25-5", "java:PatchB", () -> executedOrder.add("4.25-5:B"));

            doReturn(Arrays.asList(p4254, p4255a, p4255b))
                    .when(spyPatchesRunner).scanPatches(eq(io.tapdata.utils.AppType.DAAS), any(), any(), any(), any());

            try (MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                initLogMapMock.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
            }

            Assertions.assertEquals(Arrays.asList("4.25-4:script", "4.25-5:A", "4.25-5:B"), executedOrder);
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "script:4.25-4.json");
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "java:PatchA");
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "java:PatchB");

            ArgumentCaptor<VersionDto> dtoCaptor = ArgumentCaptor.forClass(VersionDto.class);
            verify(versionService, times(2)).upsert(any(Query.class), dtoCaptor.capture());
            Assertions.assertEquals(Arrays.asList("4.25-4", "4.25-5"),
                    dtoCaptor.getAllValues().stream().map(VersionDto::getVersion).toList());
        }

        @Test
        void testPartialGroupFailureDoesNotAdvanceVersionAndRestartResumesMissingPatch() {
            VersionDto beforeFailure = new VersionDto(daasScriptType.toString(), "4.25-3");
            when(versionService.findOne(any(Query.class))).thenReturn(beforeFailure);

            List<String> executedOrder = new ArrayList<>();
            IPatch p4254 = dummyPatch(daasScriptType, "4.25-4", "script:4.25-4.json", () -> executedOrder.add("4.25-4"));
            IPatch p4255a = dummyPatch(daasScriptType, "4.25-5", "java:PatchA", () -> executedOrder.add("4.25-5:A"));
            IPatch p4255bFailing = dummyPatch(daasScriptType, "4.25-5", "java:PatchB", () -> {
                throw new IllegalStateException("boom");
            });

            doReturn(Arrays.asList(p4254, p4255a, p4255bFailing))
                    .when(spyPatchesRunner).scanPatches(eq(io.tapdata.utils.AppType.DAAS), any(), any(), any(), any());

            try (MockedStatic<TMStartMsgUtil> startMsgMock = mockStatic(TMStartMsgUtil.class);
                 MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                Assertions.assertThrows(RuntimeException.class, () -> spyPatchesRunner.run(applicationArguments));
                startMsgMock.verify(() -> TMStartMsgUtil.writeTMStartMsg(any()), times(1));
                initLogMapMock.verify(() -> InitLogMap.complete(any()), never());
            }

            // 4.25-4 succeeded and advanced version; 4.25-5 only recorded PatchA and must NOT advance version to 4.25-5
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "script:4.25-4.json");
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "java:PatchA");
            verify(versionService, never()).recordExecutedPatch(daasScriptType.toString(), "java:PatchB");
            ArgumentCaptor<VersionDto> firstRunUpdates = ArgumentCaptor.forClass(VersionDto.class);
            verify(versionService, times(1)).upsert(any(Query.class), firstRunUpdates.capture());
            Assertions.assertEquals("4.25-4", firstRunUpdates.getValue().getVersion());

            // Simulate restart after fix: version=4.25-4 and PatchA/4.25-4 are already recorded
            reset(versionService, spyPatchesRunner);
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "versionService", versionService);
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "mongodbUri", "mongodb://localhost:27017/unused");
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "productList", Collections.singletonList("idaas"));

            VersionDto onRestart = new VersionDto(daasScriptType.toString(), "4.25-4",
                    Arrays.asList("script:4.25-4.json", "java:PatchA"));
            when(versionService.findOne(any(Query.class))).thenReturn(onRestart);

            List<String> secondRunOrder = new ArrayList<>();
            IPatch p4254Restart = dummyPatch(daasScriptType, "4.25-4", "script:4.25-4.json", () -> secondRunOrder.add("4.25-4"));
            IPatch p4255aRestart = dummyPatch(daasScriptType, "4.25-5", "java:PatchA", () -> secondRunOrder.add("4.25-5:A"));
            IPatch p4255bFixed = dummyPatch(daasScriptType, "4.25-5", "java:PatchB", () -> secondRunOrder.add("4.25-5:B"));
            doReturn(Arrays.asList(p4254Restart, p4255aRestart, p4255bFixed))
                    .when(spyPatchesRunner).scanPatches(eq(io.tapdata.utils.AppType.DAAS), any(), any(), any(), any());

            try (MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                initLogMapMock.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
            }

            Assertions.assertEquals(Collections.singletonList("4.25-5:B"), secondRunOrder);
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "java:PatchB");
            verify(versionService, never()).recordExecutedPatch(daasScriptType.toString(), "java:PatchA");
            ArgumentCaptor<VersionDto> secondRunUpdates = ArgumentCaptor.forClass(VersionDto.class);
            verify(versionService, times(1)).upsert(any(Query.class), secondRunUpdates.capture());
            Assertions.assertEquals("4.25-5", secondRunUpdates.getValue().getVersion());
        }

        @Test
        void testMissingPatchAtSameCurrentVersionRunsWithoutReRunningRecordedPatches() {
            VersionDto atLatest = new VersionDto(daasScriptType.toString(), "4.25-5",
                    Arrays.asList("script:4.25-4.json", "java:PatchA"));
            when(versionService.findOne(any(Query.class))).thenReturn(atLatest);

            List<String> executedOrder = new ArrayList<>();
            IPatch p4254 = dummyPatch(daasScriptType, "4.25-4", "script:4.25-4.json", () -> executedOrder.add("4.25-4"));
            IPatch p4255a = dummyPatch(daasScriptType, "4.25-5", "java:PatchA", () -> executedOrder.add("4.25-5:A"));
            IPatch p4255b = dummyPatch(daasScriptType, "4.25-5", "java:PatchB", () -> executedOrder.add("4.25-5:B"));

            doReturn(Arrays.asList(p4254, p4255a, p4255b))
                    .when(spyPatchesRunner).scanPatches(eq(io.tapdata.utils.AppType.DAAS), any(), any(), any(), any());

            try (MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                initLogMapMock.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
            }

            Assertions.assertEquals(Collections.singletonList("4.25-5:B"), executedOrder);
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), "java:PatchB");
            verify(versionService, never()).upsert(any(Query.class), any(VersionDto.class));
        }

        @Test
        void testHistoricalPatchesBelowTrackedBaselineNeverRescanOrReRun() {
            io.tapdata.utils.AppType daas = io.tapdata.utils.AppType.DAAS;
            ScriptPatchScanner scriptScanner = new ScriptPatchScanner(daas, Collections.emptyMap());
            JavaPatchScanner javaScanner = new JavaPatchScanner(daas);

            List<IPatch> scanned = spyPatchesRunner.scanPatches(
                    daas, PatchVersion.valueOf("4.25-2"), PatchVersion.valueOf("4.25-5"),
                    scriptScanner, javaScanner);

            Assertions.assertTrue(scanned.stream().allMatch(p -> p.version().compareTo(PatchVersion.valueOf("4.25-2")) > 0),
                    "Historical patches <= 4.25-2 must never be rescanned on upgrade");
            Assertions.assertTrue(scanned.stream().anyMatch(p -> "script:4.25-4.json".equals(p.patchId())));
            Assertions.assertTrue(scanned.stream().anyMatch(p ->
                    "java:com.tapdata.tm.init.patches.daas.V4_25_3_AuditAndRecovery:4.25-3".equals(p.patchId())));
            Assertions.assertTrue(scanned.stream().anyMatch(p ->
                    "java:com.tapdata.tm.init.patches.daas.V4_25_3_DashboardMetricsIndexes:4.25-5".equals(p.patchId())));

            // When DFS is already at legacy max version (3.20-1 < tracked baseline 3.20-2), run skips without scanning
            clearInvocations(spyPatchesRunner);
            PatchType dfsType = new PatchType(io.tapdata.utils.AppType.DFS, PatchTypeEnums.Script);
            UnitTestUtils.injectField(PatchesRunner.class, spyPatchesRunner, "productList", Arrays.asList("idaas", "dfs"));
            when(versionService.findOne(any(Query.class)))
                    .thenReturn(new VersionDto(daasScriptType.toString(), "4.25-6"))
                    .thenReturn(new VersionDto(dfsType.toString(), "3.20-1"));

            try (MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                initLogMapMock.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
            }
            verify(spyPatchesRunner, never()).scanPatches(any(), any(), any(), any(), any());
        }

        @Test
        void testDeterministicOrderWithinSameVersionPlacesScriptBeforeJava() {
            PatchVersion version = PatchVersion.valueOf("4.25-4");
            JsonFilePatch scriptPatch = new JsonFilePatch(daasScriptType, version, "4.25-4.json", "[]", Collections.emptyMap());
            IPatch javaAlpha = dummyPatch(daasScriptType, "4.25-4", "java:Alpha", () -> {});
            IPatch javaBeta = dummyPatch(daasScriptType, "4.25-4", "java:Beta", () -> {});

            IPatchScanner reverseScanner = (patches, isVersion) -> {
                patches.add(javaBeta);
                patches.add(scriptPatch);
                patches.add(javaAlpha);
            };
            List<IPatch> sorted = spyPatchesRunner.scanPatches(io.tapdata.utils.AppType.DAAS,
                    PatchVersion.valueOf("4.25-3"), PatchVersion.valueOf("4.25-4"), reverseScanner);

            Assertions.assertEquals(
                    Arrays.asList("script:4.25-4.json", "java:Alpha", "java:Beta"),
                    sorted.stream().map(IPatch::patchId).toList());
        }

        @Test
        void testSameJavaPatchClassBumpedVersionProducesDistinctPatchIdAndRuns() {
            List<String> executed = new ArrayList<>();
            IPatch v4255 = new VersionedJavaPatch(daasScriptType, PatchVersion.valueOf("4.25-5"), () -> executed.add("4.25-5"));
            IPatch v4256 = new VersionedJavaPatch(daasScriptType, PatchVersion.valueOf("4.25-6"), () -> executed.add("4.25-6"));

            String expectedV4255Id = "java:" + VersionedJavaPatch.class.getName() + ":4.25-5";
            String expectedV4256Id = "java:" + VersionedJavaPatch.class.getName() + ":4.25-6";
            Assertions.assertEquals(expectedV4255Id, v4255.patchId());
            Assertions.assertEquals(expectedV4256Id, v4256.patchId());
            Assertions.assertNotEquals(v4255.patchId(), v4256.patchId());

            VersionDto at4255 = new VersionDto(daasScriptType.toString(), "4.25-5",
                    Collections.singletonList(expectedV4255Id));
            when(versionService.findOne(any(Query.class))).thenReturn(at4255);
            doReturn(Arrays.asList(v4255, v4256))
                    .when(spyPatchesRunner).scanPatches(eq(io.tapdata.utils.AppType.DAAS), any(), any(), any(), any());

            try (MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                initLogMapMock.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
            }

            Assertions.assertEquals(Collections.singletonList("4.25-6"), executed);
            verify(versionService).recordExecutedPatch(daasScriptType.toString(), expectedV4256Id);
            verify(versionService, never()).recordExecutedPatch(daasScriptType.toString(), expectedV4255Id);
            ArgumentCaptor<VersionDto> updates = ArgumentCaptor.forClass(VersionDto.class);
            verify(versionService, times(1)).upsert(any(Query.class), updates.capture());
            Assertions.assertEquals("4.25-6", updates.getValue().getVersion());
        }

        @Test
        void testRealScanAtTrackedCurrentVersionIncludesOnlyTrackedWindowAndSkipsRecordedPatches() {
            io.tapdata.utils.AppType daas = io.tapdata.utils.AppType.DAAS;
            List<IPatch> realScanned = spyPatchesRunner.scanPatches(
                    daas, PatchVersion.valueOf("4.25-5"), PatchVersion.valueOf("4.25-5"),
                    new ScriptPatchScanner(daas, Collections.emptyMap()),
                    new JavaPatchScanner(daas));

            List<String> expectedIds = Arrays.asList(
                    "script:4.25-4.json",
                    "java:com.tapdata.tm.init.patches.daas.V4_25_3_DashboardMetricsIndexes:4.25-5");
            Assertions.assertEquals(expectedIds, realScanned.stream().map(IPatch::patchId).toList());

            VersionDto alreadyUpToDate = new VersionDto(daasScriptType.toString(), "4.25-5", expectedIds);
            when(versionService.findOne(any(Query.class))).thenReturn(alreadyUpToDate);
            clearInvocations(spyPatchesRunner);

            try (MockedStatic<InitLogMap> initLogMapMock = mockStatic(InitLogMap.class)) {
                initLogMapMock.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                Assertions.assertDoesNotThrow(() -> spyPatchesRunner.run(applicationArguments));
                initLogMapMock.verify(() -> InitLogMap.complete(any()), times(1));
            }

            verify(spyPatchesRunner, times(1)).scanPatches(
                    eq(daas), eq(PatchVersion.valueOf("4.25-5")), eq(PatchVersion.valueOf("4.25-5")), any(), any());
            verify(versionService, never()).recordExecutedPatch(any(), any());
            verify(versionService, never()).upsert(any(Query.class), any(VersionDto.class));
        }

        private IPatch dummyPatch(PatchType type, String version, String patchId, Runnable action) {
            return new AbsPatch(type, PatchVersion.valueOf(version)) {
                @Override
                public String patchId() {
                    return patchId;
                }

                @Override
                public void run() {
                    action.run();
                }
            };
        }

        private static class VersionedJavaPatch extends AbsPatch {
            private final Runnable action;

            VersionedJavaPatch(PatchType type, PatchVersion version, Runnable action) {
                super(type, version);
                this.action = action;
            }

            @Override
            public void run() {
                action.run();
            }
        }
    }
}
