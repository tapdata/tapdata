package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.IPatch;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.PatchesRunner;
import com.tapdata.tm.init.scanners.JavaPatchScanner;
import io.tapdata.utils.AppType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class V4_25_3_DashboardMetricsIndexesScannerTest {

    @Test
    void scannerSelectsPatchAfterTheCurrentIdaasVersion() {
        List<IPatch> patches = new ArrayList<>();
        PatchVersion sourceVersion = PatchVersion.valueOf("4.25-3");
        PatchVersion storedVersion = PatchVersion.valueOf("4.25-2");
        JavaPatchScanner scanner = new JavaPatchScanner(AppType.DAAS);

        scanner.scanPatches(patches,
                PatchesRunner.getVersionChecker(AppType.DAAS, sourceVersion, storedVersion));

        assertTrue(patches.stream().anyMatch(V4_25_3_DashboardMetricsIndexes.class::isInstance));
    }
}
