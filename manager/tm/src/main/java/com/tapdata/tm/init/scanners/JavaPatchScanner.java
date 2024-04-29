package com.tapdata.tm.init.scanners;

import com.tapdata.tm.init.*;
import com.tapdata.tm.init.ex.InitJavaPatchException;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.init.patches.PatchAnnotations;
import io.tapdata.utils.AppType;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

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
    public void scanPatches(@NonNull List<IPatch> patches, @NonNull Predicate<PatchVersion> isVersion) {
        Reflections ref = new Reflections(SCAN_PACKAGE);

        Set<Class<?>> classSet = ref.getTypesAnnotatedWith(PatchAnnotation.class);
        classSet.addAll(ref.getTypesAnnotatedWith(PatchAnnotations.class));
        for (Class<?> c : classSet) {
            PatchAnnotation[] patchAnnotations = c.getAnnotationsByType(PatchAnnotation.class);

            for (PatchAnnotation ins : patchAnnotations) {
                if (!patchType.inAppTypes(ins.appType())) continue;

                PatchVersion patchVersion = PatchVersion.valueOf(ins.version());
                if (!isVersion.test(patchVersion)) {
                    logger.debug("The init script has been executed {}, skip...", ins.version());
                    break;
                }

                try {
                    Constructor<?> con = c.getConstructor(PatchType.class, PatchVersion.class);
                    patches.add((IPatch) con.newInstance(patchType, patchVersion));
                } catch (Exception e) {
                    throw new InitJavaPatchException(e, c.getName());
                }
            }
        }
    }
}
