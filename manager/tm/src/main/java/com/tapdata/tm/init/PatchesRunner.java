package com.tapdata.tm.init;

import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import com.tapdata.tm.init.patches.JsonFilePatch;
import com.tapdata.tm.init.scanners.JavaPatchScanner;
import com.tapdata.tm.init.scanners.ScriptPatchScanner;
import com.tapdata.tm.utils.TMStartMsgUtil;
import com.tapdata.tm.utils.TmStartMsg;
import com.tapdata.tm.verison.dto.VersionDto;
import com.tapdata.tm.verison.service.VersionService;
import io.tapdata.utils.AppType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 16:26 Create
 */
@Slf4j
@Component
@Order(Integer.MIN_VALUE)
public class PatchesRunner implements ApplicationRunner {
    private static final Logger logger = LogManager.getLogger(PatchesRunner.class);
    private static final PatchVersion DAAS_TRACKED_MIN_VERSION = new PatchVersion(4, 25, 4);
    private static final PatchVersion DFS_TRACKED_MIN_VERSION = new PatchVersion(3, 20, 2);
    private static final PatchVersion DRS_TRACKED_MIN_VERSION = new PatchVersion(0, 0, 1);

    @Autowired
    private VersionService versionService;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;

    @Value("${spring.data.mongodb.default.uri}")
    private String mongodbUri;


    @PostConstruct
    public void init() {
        InitLogMap.register(this.getClass());
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // Add script patch replace map.
        Map<String, String> allVariables = new HashMap<>();
        allVariables.put("TAPDATA.MONGODB.URI", mongodbUri);

        executePatchByType(AppType.DAAS, allVariables);
        if (productList.contains("dfs")) {
            executePatchByType(AppType.DFS, allVariables);
        } else if (productList.contains("drs")) {
            executePatchByType(AppType.DRS, allVariables);
        }

        InitLogMap.complete(this.getClass());
    }

    private void executePatchByType(AppType appType, Map<String, String> allVariables) {
        PatchType patchType = new PatchType(appType, PatchTypeEnums.Script);
        VersionDto appVersionDto = getAppVersionDto(patchType);
        PatchVersion appVersion = (null != appVersionDto && null != appVersionDto.getVersion())
                ? PatchVersion.valueOf(appVersionDto.getVersion()) : null;
        Set<String> executedPatches = (null != appVersionDto && null != appVersionDto.getExecutedPatches())
                ? new HashSet<>(appVersionDto.getExecutedPatches()) : new HashSet<>();
        PatchVersion currentVersion = getCurrentVersion(appType);
        if (null != appVersion && (currentVersion.compareTo(appVersion) < 0
                || (currentVersion.compareTo(appVersion) == 0 && !isTrackedVersion(appType, currentVersion)))) {
            logger.info("Not have any {} patches, current version is {}", appType, currentVersion);
            return;
        }

        // scan patches
        List<IPatch> patches = scanPatches(appType, appVersion, currentVersion
                , new ScriptPatchScanner(appType, allVariables)
                , new JavaPatchScanner(appType)
        );

        // execute patches
        logger.info("Found all {} patch size: {}", appType, patches.size());
        Map<PatchVersion, List<IPatch>> groupedPatches = new TreeMap<>();
        for (IPatch patch : patches) {
            if (isTrackedVersion(appType, patch.version()) && executedPatches.contains(patch.patchId())) {
                logger.info("Patch {} {} already recorded in executedPatches, skip.", patch.patchId(), patch.version());
                continue;
            }
            groupedPatches.computeIfAbsent(patch.version(), key -> new ArrayList<>()).add(patch);
        }

        for (Map.Entry<PatchVersion, List<IPatch>> entry : groupedPatches.entrySet()) {
            PatchVersion groupVersion = entry.getKey();
            for (IPatch patch : entry.getValue()) {
                try {
                    patch.run();
                    if (isTrackedVersion(appType, groupVersion)) {
                        versionService.recordExecutedPatch(patchType.toString(), patch.patchId());
                        executedPatches.add(patch.patchId());
                    }
                } catch (Exception e) {
                    String msg = String.format("Patch %s %s error: %s", patch.type(), patch.version(), e.getMessage());
                    logger.error(msg);
                    TmStartMsg tmMsg = new TmStartMsg("fail", msg);
                    TMStartMsgUtil.writeTMStartMsg(tmMsg);
                    throw new RuntimeException(msg, e);
                }
            }
            if (null == appVersion || groupVersion.compareTo(appVersion) > 0) {
                setAppVersion(patchType, groupVersion);
                appVersion = groupVersion;
            }
        }
        if (null == appVersion || currentVersion.compareTo(appVersion) > 0) {
            setAppVersion(patchType, currentVersion);
        }
        logger.info("Patch {} {} completed.", patchType, currentVersion);
    }

    /**
     * Get current version
     *
     * @return current version
     */
    protected PatchVersion getCurrentVersion(AppType appType) {
        PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
        String versionFilePath = String.format("%s/version", patchDir(appType));
        Resource versionRes = pathMatchingResourcePatternResolver.getResource(versionFilePath);
        if (!versionRes.exists()) {
            throw new RuntimeException("Get current version failed, because version file '" + versionFilePath + "' is not exists.");
        }
        String version;
        try (InputStream is = versionRes.getInputStream()) {
            version = IoUtil.read(is, StandardCharsets.UTF_8);
        } catch (IORuntimeException | IOException e) {
            throw new RuntimeException("Error reading version file", e);
        }
        return PatchVersion.valueOf(version);
    }

    /**
     * Get application version record
     *
     * @return version dto
     */
    private VersionDto getAppVersionDto(PatchType patchType) {
        return versionService.findOne(Query.query(Criteria.where("type").is(patchType.toString())));
    }

    /**
     * Store completed versions
     *
     * @param patchType    completed type
     * @param patchVersion completed version
     */
    private void setAppVersion(PatchType patchType, PatchVersion patchVersion) {
        VersionDto versionDto = new VersionDto(patchType.toString(), patchVersion.toString());
        versionService.upsert(Query.query(Criteria.where("type").is(versionDto.getType())), versionDto);
    }

    /**
     * Scan patches
     *
     * @param appVersion  application version
     * @param softVersion current version
     * @param scanners    scanner instance
     * @return patches
     */
    protected List<IPatch> scanPatches(AppType appType, PatchVersion appVersion, PatchVersion softVersion, IPatchScanner... scanners) {
        List<IPatch> patches = new ArrayList<>();

        Predicate<PatchVersion> versionChecker = getVersionChecker(appType, softVersion, appVersion);
        Predicate<PatchVersion> isVersion = versionChecker.or(
                patchVersion -> isTrackedVersion(appType, patchVersion) && patchVersion.compareTo(softVersion) <= 0
        );

        for (IPatchScanner scanner : scanners) {
            scanner.scanPatches(patches, isVersion);
        }

        patches.sort(Comparator.comparing(IPatch::version)
                .thenComparingInt(patch -> patch instanceof JsonFilePatch ? 0 : 1)
                .thenComparing(IPatch::patchId));
        return patches;
    }

    public static boolean isTrackedVersion(@NonNull AppType appType, @NonNull PatchVersion version) {
        PatchVersion trackedMinVersion = trackedMinVersion(appType);
        return null != trackedMinVersion && version.compareTo(trackedMinVersion) >= 0;
    }

    public static PatchVersion trackedMinVersion(@NonNull AppType appType) {
        switch (appType) {
            case DAAS:
                return DAAS_TRACKED_MIN_VERSION;
            case DFS:
                return DFS_TRACKED_MIN_VERSION;
            case DRS:
                return DRS_TRACKED_MIN_VERSION;
            default:
                return null;
        }
    }

    public static String patchDir(@NonNull AppType appType) {
        switch (appType) {
            case DFS:
                return ResourceUtils.CLASSPATH_URL_PREFIX + "init/dfs";
            case DRS:
                return ResourceUtils.CLASSPATH_URL_PREFIX + "init/drs";
            default:
                break;
        }
        return ResourceUtils.CLASSPATH_URL_PREFIX + "init/idaas";
    }

    public static Predicate<PatchVersion> getVersionChecker(AppType appType, PatchVersion softVersion, PatchVersion appVersion) {
        if (null == appVersion) {
            logger.info("Scan {} patches in last version: {}", appType, softVersion);
            return patchVersion -> patchVersion.compareTo(softVersion) <= 0;
        } else {
            logger.info("Scan {} patches from {} to {}", appType, appVersion, softVersion);
            return patchVersion -> patchVersion.compareTo(appVersion) > 0 && patchVersion.compareTo(softVersion) <= 0;
        }
    }

}
