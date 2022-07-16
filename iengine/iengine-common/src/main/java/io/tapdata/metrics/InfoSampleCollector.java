package io.tapdata.metrics;

import com.google.common.collect.ImmutableMap;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.InfoCollector;
import io.tapdata.common.sample.InfoReporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Dexter
 */
public class InfoSampleCollector {
  public void registerInfo(ConfigurationCenter configCenter, String version) {
    Map<String, String> tags = new HashMap<>();
    tags.put("host", MetricsUtil.getHostName());
    tags.put("agentId", MetricsUtil.getProcessId());
    tags.put("customerId", (String)configCenter.getConfig("userId"));

    InfoCollector infoCollector = CollectorFactory.getInstance().getDisposableCollectorByTags("info", tags);

    infoCollector.addInfo("agentVersion", () -> version);
    String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
    if (tapdataWorkDir != null) {
      infoCollector.addInfo("path", () -> tapdataWorkDir);
      File file = new File(tapdataWorkDir);
      infoCollector.addInfo("diskTotal", file::getTotalSpace);
      infoCollector.addInfo("freeDisk", file::getFreeSpace);
    }
    infoCollector.addInfo("agentMaxMem", () ->
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()
    );
    infoCollector.addInfo("limitedCores", () -> Runtime.getRuntime().availableProcessors());

    SystemInfo sysInfo = new SystemInfo();
    HardwareAbstractionLayer hardware = sysInfo.getHardware();
    OperatingSystem operatingSystem = sysInfo.getOperatingSystem();
    infoCollector.addInfo("memory", () -> hardware.getMemory().getTotal());
    infoCollector.addInfo("cpu", () -> new HashMap<String, Object>(3) {{
      put("model", hardware.getProcessor().getProcessorIdentifier().getVendor());
      put("cores", hardware.getProcessor().getLogicalProcessorCount());
      put("mhz", hardware.getProcessor().getMaxFreq());
    }});
    infoCollector.addInfo("OS", () -> operatingSystem.getVersionInfo().toString());
    infoCollector.addInfo("JDK", () ->
      System.getProperty("java.runtime.name") + System.getProperty("java.version")
    );
  }

  public static class AgentInfoReporter implements InfoReporter {
    private final Logger logger = LogManager.getLogger(TaskSampleReporter.class);

    private ClientMongoOperator operator;

    public AgentInfoReporter(ClientMongoOperator operator) {
      this.operator = operator;
    }

    @Override
    public void execute(Map<String, Object> pointValues, Map<String, String> tags) {
      if (pointValues.isEmpty()) {
        logger.info("The info samples request is empty, skip report process.");
      }

      try {
        operator.insertOne(
          Collections.singletonList(ImmutableMap.of("tags", tags, "values", pointValues)),
          ConnectorConstant.AGENT_INFO_COLLECTION + "/add");
      } catch (Exception e) {
        logger.warn("Failed to report task samples and statistics, will retry...");
      }
    }
  }
}
