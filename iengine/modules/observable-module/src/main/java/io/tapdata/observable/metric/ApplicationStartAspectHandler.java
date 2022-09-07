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

import java.lang.management.ManagementFactory;
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
        collector.addSampler("cpuUsage", () ->
                ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class).getProcessCpuLoad()
        );
        collector.addSampler("memUsed", () ->
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()
        );
        collector.addSampler("physicalMemTotal", () ->
                ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize()
        );
        collector.addSampler("memoryRate", () ->
                (double) ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() / ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize()
        );

        GcSampler gcSamplerTime = new GcSampler(GcSampler.GcPointEnum.GC_TIME);
        gcSamplerTime.start();
        collector.addSampler("gcTimeIn5Min", gcSamplerTime);
        collector.addSampler("gcRate", () ->
            gcSamplerTime.value().doubleValue() / 300000
        );
    }
}
