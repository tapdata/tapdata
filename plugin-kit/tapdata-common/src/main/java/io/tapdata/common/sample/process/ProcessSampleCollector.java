package io.tapdata.common.sample.process;

import com.sun.management.OperatingSystemMXBean;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.SampleReporter;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Dexter
 */
public class ProcessSampleCollector {
    private final String prefix;
    private final SampleCollector collector;

    public ProcessSampleCollector(String prefix, SampleReporter reporter) {
        this(prefix, reporter, false);
    }

    public ProcessSampleCollector(String prefix, SampleReporter reporter, boolean addAll) {
        this.prefix = prefix;
        collector = new SampleCollector(reporter)
                .withName(prefix + "Process")
                .withTag("measureType", "process")
                .withTag("processType", prefix)
        ;
        init(addAll);
    }

    private void init(boolean addAll) {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        String[] splitName = bean.getName().split("@");
        String pid = splitName.length >= 1 ? splitName[0] : null;
        String host = splitName.length >= 2 ? splitName[0] : null;
        if (pid != null) {
            collector.withTag("processId", pid);
        }
        if (host != null) {
            collector.withTag("host", host);
        }


        if (addAll) {
            this.withMemUsage()
                    .withCpuUsage()
                    .withGcTime()
                    .withGcCount();

        }
        collector.start();
    }

    public ProcessSampleCollector(String prefix, boolean addAll) {
        this.prefix = prefix;
        Map<String, String> tags = new HashMap<>();
        tags.put("measureType", "process");
        tags.put("processType", prefix);
        collector = CollectorFactory.getInstance().getSampleCollectorByTags(prefix + "Process", tags);

        init(addAll);
    }

    public ProcessSampleCollector withMemUsage() {
        collector.addSampler(prefix + "MemUsage", () ->
            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()
        );
        return this;
    }

    public ProcessSampleCollector withCpuUsage() {
        collector.addSampler(prefix + "CpuUsage", () ->
            ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class).getProcessCpuLoad()
        );
        return this;
    }

    public ProcessSampleCollector withGcTime() {
        GcSampler gcSamplerTime = new GcSampler(GcSampler.GcPointEnum.GC_TIME);
        gcSamplerTime.start();
        collector.addSampler(prefix + "GcTimeIn5Min", gcSamplerTime);

        return this;
    }

    public ProcessSampleCollector withGcCount() {
        GcSampler gcSamplerCount = new GcSampler(GcSampler.GcPointEnum.GC_COUNT);
        gcSamplerCount.start();
        collector.addSampler(prefix + "GcTimeIn5Min", gcSamplerCount);

        return this;
    }


}
