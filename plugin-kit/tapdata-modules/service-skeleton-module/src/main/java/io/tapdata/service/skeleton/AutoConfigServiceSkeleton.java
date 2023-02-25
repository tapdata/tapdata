package io.tapdata.service.skeleton;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.modules.api.service.SkeletonService;
import io.tapdata.pdk.core.utils.AnnotationUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

@MainMethod(value = "main", order = 10)
@Bean
public class AutoConfigServiceSkeleton {
    private static final String TAG = AutoConfigServiceSkeleton.class.getSimpleName();
    @Bean
    private ServiceSkeletonAnnotationHandler serviceSkeletonAnnotationHandler;
    
    private void main() {
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .addScanners(new TypeAnnotationsScanner())
//                .forPackages(this.scanPackages)
                .addClassLoader(this.getClass().getClassLoader());
        String scanPackage = CommonUtils.getProperty("service_skeleton_scan_package", "io.tapdata");
        String[] packages = scanPackage.split(",");

        builder.forPackages(packages);
        Reflections reflections = new Reflections(builder);

        serviceSkeletonAnnotationHandler.setService(SkeletonService.SERVICE_ENGINE);
        TapLogger.debug(TAG, "Start scanning service skeleton classes");
        AnnotationUtils.runClassAnnotationHandlers(reflections, new ClassAnnotationHandler[]{
                serviceSkeletonAnnotationHandler
        }, TAG);
    }
    
}