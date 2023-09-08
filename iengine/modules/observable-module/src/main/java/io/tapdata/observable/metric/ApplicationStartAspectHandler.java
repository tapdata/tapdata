package io.tapdata.observable.metric;

import cn.hutool.core.date.DateUtil;
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
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.HashMap;

/**
 * @author Dexter
 */
@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
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
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }

        ScriptEngine scriptEnginePy = scriptFactory.create(ScriptFactory.TYPE_PYTHON, new ScriptOptions().engineName(ScriptFactory.TYPE_PYTHON));
        try {
            scriptEnginePy.eval("import sys\n" +
                    "builtin_modules = sys.builtin_module_names\n" +
                    "all_packages_arr = []\n" +
                    "for module_name in builtin_modules:\n" +
                    "    all_packages_arr.append(module_name)\n" +
                    "all_packages_str = ', '.join(all_packages_arr)\n" +
                    "print ('[INFO ] Python engine has loaded, support system packages: ' + all_packages_str) ");
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }
}
