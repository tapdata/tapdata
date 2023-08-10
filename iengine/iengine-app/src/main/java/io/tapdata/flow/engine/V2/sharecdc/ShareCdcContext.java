package io.tapdata.flow.engine.V2.sharecdc;

import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.observable.logging.ObsLogger;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 14:33
 **/
public class ShareCdcContext implements Serializable {

	private static final long serialVersionUID = -7643172217247692560L;

	// Incremental start timestamp
	private Long cdcStartTs;

	// Tapdata Settings
	private ConfigurationCenter configurationCenter;
	private ObsLogger obsLogger;
	private List<String> tables;

	public ShareCdcContext(Long cdcStartTs, ConfigurationCenter configurationCenter, List<String> tables) {
		if (null == cdcStartTs || cdcStartTs.compareTo(0L) < 0) {
			this.cdcStartTs = 0L;
		} else {
			this.cdcStartTs = cdcStartTs;
		}
		this.configurationCenter = configurationCenter;
		this.tables = tables;
	}

	public Long getCdcStartTs() {
		return cdcStartTs;
	}

	public ConfigurationCenter getConfigurationCenter() {
		return configurationCenter;
	}

	public ObsLogger getObsLogger() {
		return obsLogger;
	}

	public void setObsLogger(ObsLogger obsLogger) {
		this.obsLogger = obsLogger;
	}

	public List<String> getTables() {
		return tables;
	}
}
