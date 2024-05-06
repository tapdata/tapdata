package com.tapdata.tm.init.scanners;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import com.tapdata.tm.init.*;
import com.tapdata.tm.init.ex.FindScriptPatchException;
import com.tapdata.tm.init.ex.ReadScriptPatchException;
import com.tapdata.tm.init.patches.JsonFilePatch;
import io.tapdata.utils.AppType;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 16:22 Create
 */
public class ScriptPatchScanner implements IPatchScanner {
    private static final Logger logger = LogManager.getLogger(ScriptPatchScanner.class);
    private final PatchType patchType;

    private final Map<String, String> allVariables;

    public ScriptPatchScanner(AppType appType, Map<String, String> allVariables) {
        this.patchType = new PatchType(appType, PatchTypeEnums.Script);
        this.allVariables = allVariables;
    }

    @Override
    public void scanPatches(@NonNull List<IPatch> patches, @NonNull Predicate<PatchVersion> isVersion) {
        switch (patchType.getAppType()) {
            case DFS:
                parse(patches, PatchesRunner.patchDir(AppType.DFS) + "/*.json", isVersion);
                break;
            case DRS:
                parse(patches, PatchesRunner.patchDir(AppType.DRS) + "/*.json", isVersion);
                break;
            default:
                parse(patches, PatchesRunner.patchDir(AppType.DAAS) + "/*.json", isVersion);
                break;
        }
    }

    private void parse(List<IPatch> patches, String path, Predicate<PatchVersion> isVersion) {
        for (Resource resource : getResources(path)) {
            String currentVersion = FileUtil.mainName(resource.getFilename());
            if (null == currentVersion) {
                logger.warn("Script version is null, skip script file: {}", resource.getFilename());
                continue;
            }
            PatchVersion version = PatchVersion.valueOf(currentVersion);
            if (Boolean.FALSE.equals(isVersion.test(version))) {
                logger.debug("The init script has been executed {}, skip...", currentVersion);
                continue;
            }
            try (InputStream is = resource.getInputStream()) {
                String scriptStr = IoUtil.read(is, StandardCharsets.UTF_8);
                patches.add(new JsonFilePatch(patchType, version, resource.getFilename(), scriptStr, allVariables));
            } catch (IORuntimeException | IOException e) {
                throw new ReadScriptPatchException(e, resource.getFilename());
            }
        }
    }

    protected Resource[] getResources(String path) {
        try {
            PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
            return pathMatchingResourcePatternResolver.getResources(path);
        } catch (IOException e) {
            throw new FindScriptPatchException(e , path);
        }
    }
}
