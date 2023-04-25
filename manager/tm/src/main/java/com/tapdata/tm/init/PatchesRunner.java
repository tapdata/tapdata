package com.tapdata.tm.init;

import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import com.tapdata.tm.init.scanners.JavaPatchScanner;
import com.tapdata.tm.init.scanners.ScriptPatchScanner;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.verison.dto.VersionDto;
import com.tapdata.tm.verison.service.VersionService;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 16:26 Create
 */
@Slf4j
@Component
@Order(Integer.MIN_VALUE)
public class PatchesRunner implements ApplicationRunner {
    private static final Logger logger = LogManager.getLogger(PatchesRunner.class);

    @Autowired
    private VersionService versionService;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;

    @Value("${spring.data.mongodb.default.uri}")
    private String mongodbUri;

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
    }

    private void executePatchByType(AppType appType, Map<String, String> allVariables) {
        PatchType patchType = new PatchType(appType, PatchTypeEnums.Script);
        PatchVersion appVersion = getAppVersion(patchType);
        PatchVersion currentVersion = getCurrentVersion(appType);
        if (null != appVersion && currentVersion.compareTo(appVersion) <= 0) {
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
        for (IPatch patch : patches) {
            try {
                // process and store status
                patch.run();
                setAppVersion(patch.type(), patch.version());
            } catch (Exception e) {
                String msg = String.format("Patch %s %s error: %s", patch.type(), patch.version(), e.getMessage());
                logger.error(msg);
                throw new RuntimeException(msg, e);
            }
        }
        setAppVersion(patchType, currentVersion);
        logger.info("Patch {} {} completed.", patchType, currentVersion);
    }

    /**
     * Get current version
     *
     * @return current version
     */
    private PatchVersion getCurrentVersion(AppType appType) {
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
     * Get application version
     *
     * @return application version
     */
    private PatchVersion getAppVersion(PatchType patchType) {
        VersionDto dto = versionService.findOne(Query.query(Criteria.where("type").is(patchType.toString())));
        if (null != dto) {
            return PatchVersion.valueOf(dto.getVersion());
        }
        return null;
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
    private List<IPatch> scanPatches(AppType appType, PatchVersion appVersion, PatchVersion softVersion, IPatchScanner... scanners) {
        List<IPatch> patches = new ArrayList<>();

        Function<PatchVersion, Boolean> isVersion;
        if (null == appVersion) {
            logger.info("Scan {} patches in last version: {}", appType, softVersion);
            isVersion = (patchVersion) -> patchVersion.compareTo(softVersion) <= 0;
//            isVersion = (patchVersion) -> patchVersion.compareVersion(softVersion) == 0 && patchVersion.compareTo(softVersion) <= 0;
        } else {
            logger.info("Scan {} patches from {} to {}", appType, appVersion, softVersion);
            isVersion = (patchVersion) -> patchVersion.compareTo(appVersion) > 0 && patchVersion.compareTo(softVersion) <= 0;
        }

        for (IPatchScanner scanner : scanners) {
            scanner.scanPatches(patches, isVersion);
        }

        patches.sort(null);
        return patches;
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
}
