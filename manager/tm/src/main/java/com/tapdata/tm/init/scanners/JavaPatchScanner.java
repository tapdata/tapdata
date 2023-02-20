package com.tapdata.tm.init.scanners;

import com.tapdata.tm.init.*;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.init.patches.PatchAnnotations;
import com.tapdata.tm.sdk.util.AppType;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 17:05 Create
 */
public class JavaPatchScanner implements IPatchScanner {
    private static final Logger logger = LogManager.getLogger(JavaPatchScanner.class);
    private static final String SCAN_PACKAGE = "com.tapdata.tm.init.patches";
    private final PatchType patchType;

    public JavaPatchScanner(AppType appType) {
        this.patchType = new PatchType(appType, PatchTypeEnums.Script);
    }

    @Override
    public void scanPatches(@NonNull List<IPatch> patches, @NonNull Function<PatchVersion, Boolean> isVersion) {
        Reflections ref = new Reflections(SCAN_PACKAGE);

        Set<Class<?>> classSet = ref.getTypesAnnotatedWith(PatchAnnotations.class);
        for (Class<?> c : classSet) {
            PatchAnnotation[] patchAnnotations = c.getAnnotationsByType(PatchAnnotation.class);

            for (PatchAnnotation ins : patchAnnotations) {
                if (!patchType.inAppTypes(ins.appType())) continue;

                PatchVersion patchVersion = PatchVersion.valueOf(ins.version());
                if (isVersion.apply(patchVersion)) {
                    try {
                        Constructor<?> con = c.getConstructor(PatchType.class, PatchVersion.class);
                        patches.add((IPatch) con.newInstance(patchType, patchVersion));
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("Init patch '%s' failed: %s", c.getName(), e.getMessage()), e);
                    }continue;
                } else {
                    logger.info("The init script has been executed {}, skip...", ins.version());
                }
                break;
            }
        }
    }
}
