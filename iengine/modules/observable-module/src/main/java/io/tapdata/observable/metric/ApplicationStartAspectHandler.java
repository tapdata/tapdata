package io.tapdata.observable.metric;

import com.sun.management.OperatingSystemMXBean;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.FileUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.process.GcSampler;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;

import javax.script.ScriptEngine;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

/**
 * @author Dexter
 */
@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
    public static final String TAG = ApplicationStartAspectHandler.class.getSimpleName();

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
            try {
                FileUtils.deleteDirectory(new File("py-lib"));
            } catch (Exception ignore){}
            ScriptEngine scriptEnginePy = scriptFactory.create(ScriptFactory.TYPE_PYTHON, new ScriptOptions().engineName(ScriptFactory.TYPE_PYTHON).log(new TapLog()));
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
}
