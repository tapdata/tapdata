package com.tapdata.entity.dataflow;

import com.tapdata.constant.ConnectorConstant;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-09-06 10:36
 **/
public class ModPipeline implements Serializable {

	private static final long serialVersionUID = 2598408841205222543L;

	private List<Stage> stages;

	private Type type;

	/**
	 * {@link ConnectorConstant#SYNC_TYPE_INITIAL_SYNC_CDC}
	 * {@link ConnectorConstant#SYNC_TYPE_CDC}
	 */
	private String syncType;

	public ModPipeline() {
	}

	public List<Stage> getStages() {
		return stages;
	}

	public Type getType() {
		return type;
	}

	public String getSyncType() {
		return syncType;
	}

	public enum Type {
		add,
		del,
		;
	}

	public boolean isEmpty() {
		return type == null || StringUtils.isBlank(syncType) || CollectionUtils.isEmpty(stages);
	}
}
