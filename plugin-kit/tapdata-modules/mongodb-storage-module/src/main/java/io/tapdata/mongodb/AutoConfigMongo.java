package io.tapdata.mongodb;

import com.mongodb.client.MongoClient;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.pdk.core.utils.AnnotationUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

@MainMethod(value = "main", order = 10)
@Bean
public class AutoConfigMongo {
    private static final String TAG = AutoConfigMongo.class.getSimpleName();
    @Bean
    private MongoDAOAnnotationHandler mongoDAOAnnotationHandler;
    
    private void main() {
        //扫描所有的MongoCollection
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .addScanners(new TypeAnnotationsScanner())
//                .forPackages(this.scanPackages)
                .addClassLoader(this.getClass().getClassLoader());
        String scanPackage = CommonUtils.getProperty("mongodb_scan_package", "io.tapdata.mongodb");
        String[] packages = scanPackage.split(",");

        builder.forPackages(packages);
        Reflections reflections = new Reflections(builder);

        TapLogger.debug(TAG, "Start scanning mongodb dao classes");
        AnnotationUtils.runClassAnnotationHandlers(reflections, new ClassAnnotationHandler[]{
                mongoDAOAnnotationHandler
        }, TAG);
    }
    
}