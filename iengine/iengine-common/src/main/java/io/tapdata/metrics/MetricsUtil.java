package io.tapdata.metrics;

import com.tapdata.constant.AgentUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;

/**
 * @author Dexter
 */
public class MetricsUtil {
	private static final Logger logger = LogManager.getLogger(MetricsUtil.class);

	/**
	 * Get the hostname of the machine.
	 */
	public static String getHostName() {
		String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (Exception ignore) {
		}

		return hostname;
	}

	/**
	 * Get the processId of the agent(a.k.a. agentId).
	 */
	public static String getProcessId() {
		String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
		String processId = System.getenv("process_id");
		if (StringUtils.isBlank(processId)) {
			processId = AgentUtil.readAgentId(tapdataWorkDir);
			if (StringUtils.isBlank(processId)) {
				try {
					processId = AgentUtil.createAgentIdYaml(tapdataWorkDir);
				} catch (IOException e) {
					logger.error("Generate process id failed, will exit agent, please try to set process_id in env and try again, message: {}", e.getMessage(), e);
					System.exit(1);
				}
				logger.info("Generated process id in agent.yml, please don't modify it, process id: {}", processId);
			}
		}

		return processId;
	}
}
