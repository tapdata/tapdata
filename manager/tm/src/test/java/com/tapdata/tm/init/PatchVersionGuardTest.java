package com.tapdata.tm.init;

import cn.hutool.core.io.FileUtil;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.init.patches.PatchAnnotations;
import io.tapdata.utils.AppType;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PatchVersionGuardTest {

    private static final String PATCH_SCAN_PACKAGE = "com.tapdata.tm.init.patches";

    /**
     * Legacy duplicate patch versions that shipped historically and must stay untouched.
     */
    private static final Map<String, Integer> LEGACY_DUPLICATE_WHITELIST = Map.of(
            key(AppType.DAAS, PatchVersion.valueOf("2.14-4")), 2,
            key(AppType.DAAS, PatchVersion.valueOf("2.14-5")), 2,
            key(AppType.DAAS, PatchVersion.valueOf("3.3-1")), 2,
            key(AppType.DFS, PatchVersion.valueOf("2.14-0")), 2
    );

    @Test
    void noUnexpectedDuplicatePatchVersionsAndVersionFileCoversLatest() throws IOException {
        Map<String, List<String>> patchesByAppAndVersion = collectAllPatches();
        assertNoUnexpectedCollisions(patchesByAppAndVersion, LEGACY_DUPLICATE_WHITELIST);

        assertEquals(List.of("java:com.tapdata.tm.init.patches.daas.V4_25_2_WorkerApiServerUnique"),
                patchesByAppAndVersion.get(key(AppType.DAAS, PatchVersion.valueOf("4.25-2"))));
        assertEquals(List.of("java:com.tapdata.tm.init.patches.daas.V4_25_3_AuditAndRecovery"),
                patchesByAppAndVersion.get(key(AppType.DAAS, PatchVersion.valueOf("4.25-3"))));
        assertEquals(List.of("script:4.25-4.json"),
                patchesByAppAndVersion.get(key(AppType.DAAS, PatchVersion.valueOf("4.25-4"))));
        assertEquals(List.of("java:com.tapdata.tm.init.patches.daas.V4_25_3_DashboardMetricsIndexes"),
                patchesByAppAndVersion.get(key(AppType.DAAS, PatchVersion.valueOf("4.25-5"))));

        for (AppType appType : AppType.values()) {
            PatchVersion maxPatchVersion = patchesByAppAndVersion.keySet().stream()
                    .filter(k -> k.startsWith(appType.name() + ":"))
                    .map(k -> PatchVersion.valueOf(k.substring(appType.name().length() + 1)))
                    .max(PatchVersion::compareTo)
                    .orElse(null);
            if (maxPatchVersion == null) {
                continue;
            }
            PatchVersion versionFileValue = readVersionFile(appType);
            assertTrue(versionFileValue.compareTo(maxPatchVersion) >= 0,
                    () -> String.format("Version file for %s (%s) must be >= max patch version (%s)",
                            appType, versionFileValue, maxPatchVersion));
        }
    }

    @Test
    void rejectsUnexpectedDuplicatePatchVersions() {
        Map<String, List<String>> simulated = new LinkedHashMap<>();
        String collisionKey = key(AppType.DAAS, PatchVersion.valueOf("4.25-6"));
        simulated.put(collisionKey, List.of("script:4.25-6.json", "java:com.tapdata.tm.init.patches.daas.FakePatch"));

        AssertionError error = assertThrows(AssertionError.class,
                () -> assertNoUnexpectedCollisions(simulated, LEGACY_DUPLICATE_WHITELIST));
        assertTrue(error.getMessage().contains(collisionKey));
    }

    @Test
    void rejectsExpandingLegacyDuplicateWhitelistEntries() {
        Map<String, List<String>> simulated = new LinkedHashMap<>();
        String legacyKey = key(AppType.DAAS, PatchVersion.valueOf("2.14-4"));
        simulated.put(legacyKey, List.of("script:2.14-4.json", "java:V2_14_4_DatabaseTypes", "java:ExtraPatch"));

        AssertionError error = assertThrows(AssertionError.class,
                () -> assertNoUnexpectedCollisions(simulated, LEGACY_DUPLICATE_WHITELIST));
        assertTrue(error.getMessage().contains(legacyKey));
    }

    static void assertNoUnexpectedCollisions(
            Map<String, List<String>> patchesByAppAndVersion,
            Map<String, Integer> legacyWhitelist) {
        List<String> collisions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : patchesByAppAndVersion.entrySet()) {
            List<String> sources = entry.getValue();
            if (sources.size() <= 1) {
                continue;
            }
            Integer allowedCount = legacyWhitelist.get(entry.getKey());
            if (allowedCount == null || sources.size() > allowedCount) {
                collisions.add(entry.getKey() + " -> " + sources);
            }
        }
        assertTrue(collisions.isEmpty(),
                () -> "Duplicate patch versions detected (please bump version & init/*/version): " + collisions);
    }

    private static Map<String, List<String>> collectAllPatches() throws IOException {
        Map<String, List<String>> grouped = new LinkedHashMap<>();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        for (AppType appType : AppType.values()) {
            Resource[] resources = resolver.getResources(PatchesRunner.patchDir(appType) + "/*.json");
            for (Resource resource : resources) {
                String mainName = FileUtil.mainName(resource.getFilename());
                if (mainName == null) {
                    continue;
                }
                PatchVersion version = PatchVersion.valueOf(mainName);
                grouped.computeIfAbsent(key(appType, version), k -> new ArrayList<>())
                        .add("script:" + resource.getFilename());
            }
        }

        Reflections reflections = new Reflections(PATCH_SCAN_PACKAGE);
        java.util.Set<Class<?>> patchClasses = new java.util.LinkedHashSet<>(reflections.getTypesAnnotatedWith(PatchAnnotation.class));
        patchClasses.addAll(reflections.getTypesAnnotatedWith(PatchAnnotations.class));
        for (Class<?> patchClass : patchClasses) {
            for (PatchAnnotation annotation : patchClass.getAnnotationsByType(PatchAnnotation.class)) {
                PatchVersion version = PatchVersion.valueOf(annotation.version());
                AppType appType = annotation.appType();
                grouped.computeIfAbsent(key(appType, version), k -> new ArrayList<>())
                        .add("java:" + patchClass.getName());
            }
        }

        grouped.values().forEach(Collections::sort);
        return grouped;
    }

    private static PatchVersion readVersionFile(AppType appType) throws IOException {
        String path = PatchesRunner.patchDir(appType) + "/version";
        Resource resource = new PathMatchingResourcePatternResolver().getResource(path);
        assertTrue(resource.exists(), () -> "Missing version file: " + path);
        try (InputStream is = resource.getInputStream()) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
            assertFalse(content.isEmpty(), () -> "Empty version file for " + appType + ": " + path);
            return PatchVersion.valueOf(content);
        }
    }

    private static String key(AppType appType, PatchVersion version) {
        return appType.name() + ":" + version;
    }
}
