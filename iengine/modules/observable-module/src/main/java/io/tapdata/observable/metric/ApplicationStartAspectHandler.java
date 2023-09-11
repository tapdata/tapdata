package io.tapdata.observable.metric;

import com.sun.management.OperatingSystemMXBean;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.process.GcSampler;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.FileCopyUtils;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

/**
 * @author Dexter
 */
@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
    public static final String TAG = ApplicationStartAspectHandler.class.getSimpleName();
    @Autowired
    private ResourceLoader resourceLoader;

    @Override
    public void observe(ApplicationStartAspect aspect) {
        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        CollectorFactory.getInstance("v2").start(new TaskSampleReporter(clientMongoOperator));
        RestTemplateOperator restTemplateOperator = BeanUtil.getBean(RestTemplateOperator.class);
        TaskSampleRetriever.getInstance().start(restTemplateOperator);

        ConfigurationCenter configurationCenter = BeanUtil.getBean(ConfigurationCenter.class);
        SampleCollector collector = CollectorFactory.getInstance("v2").getSampleCollectorByTags("agentSamplers", new HashMap<String, String>() {{
            put("type", "engine");
            put("engineId", (String) configurationCenter.getConfig(ConfigurationCenter.AGENT_ID));
        }});
        collector.addSampler("cpuUsage", () -> ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class).getProcessCpuLoad());
        collector.addSampler("memUsed", () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        collector.addSampler("physicalMemTotal", () ->
                ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize()
        );
        collector.addSampler("heapMemTotal", () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
        collector.addSampler("memoryRate", () -> (double) ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() / ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());

        GcSampler gcSamplerTime = new GcSampler(GcSampler.GcPointEnum.GC_TIME);
        gcSamplerTime.start();
        collector.addSampler("gcTimeIn5Min", gcSamplerTime);
        collector.addSampler("gcRate", () ->
            gcSamplerTime.value().doubleValue() / 300000
        );
        collector.addSampler("date", System::currentTimeMillis);

        final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));
        try {
            scriptEngine.eval("console.log('[INFO ] Graal.js engine has loaded');");
        } catch (Exception e) {
            TapLogger.warn(TAG, "Can not load Graal.js engine, msg: {}", e.getMessage());
        }

        try {
            copyFile();
            ScriptEngine scriptEnginePy = scriptFactory.create(ScriptFactory.TYPE_PYTHON, new ScriptOptions().engineName(ScriptFactory.TYPE_PYTHON));
            scriptEnginePy.eval("import sys\n" +
                    "builtin_modules = sys.builtin_module_names\n" +
                    "all_packages_arr = []\n" +
                    "for module_name in builtin_modules:\n" +
                    "    all_packages_arr.append(module_name)\n" +
                    "all_packages_str = ', '.join(all_packages_arr)\n" +
                    "print ('[INFO ] Python engine has loaded, support system packages: ' + all_packages_str) ");
        } catch (Exception e) {
            TapLogger.warn(TAG, "Can not load python engine, msg: {}", e.getMessage());
        }
    }

    /**
     * @deprecated
     * */
    public int copyFile() {
        if (null == resourceLoader) resourceLoader = new DefaultResourceLoader();
        final String sourceFolder = "classpath:py-libs";
        final String destinationFolder = "py-lib";
        try {
            Resource resource = resourceLoader.getResource(sourceFolder);
            File sourceDir = resource.getFile();
            File[] files = sourceDir.listFiles();
            if (null == files) return -1;
            File destFolder = new File(destinationFolder);
            if (!destFolder.exists()) {
                destFolder.mkdirs();
            }
            for (File file : files) {
                copyFile(file, new File(destFolder, file.getName()));
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, "Can not get python packages resources when load python engine, msg: {}", e.getMessage());
            return  -1;
        }
        return 1;
    }

    private void copyFile(File file, File target) throws Exception {
        if (null == file) return;
        File[] files = file.listFiles();
        if (!target.exists() || !target.isDirectory()) target.mkdirs();
        if (null == files || files.length <= 0) return;
        for (File f : files) {
            if (f.isDirectory()) {
                copyFile(f, new File(FilenameUtils.concat(target.getPath(), f.getName())));
            } else if (f.isFile()) {
                copy(f, new File(FilenameUtils.concat(target.getPath(), f.getName())));
            }
        }
    }

    private void copy(File from, File to) throws Exception{
        try (FileInputStream fis = new FileInputStream(from.getAbsolutePath());
             FileOutputStream fos = new FileOutputStream(to.getAbsolutePath())) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

}
