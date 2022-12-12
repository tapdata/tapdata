package io.tapdata.inspect;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectTask;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.pdk.core.api.ConnectorNode;

/**
 * @author samuel
 * @Description
 * @create 2022-06-06 11:47
 **/
public abstract class InspectContext {
	protected String name;
	protected com.tapdata.entity.inspect.InspectTask task;
	protected Connections source;
	protected Connections target;
	protected String inspectResultParentId;
	/**
	 * 差异结果模式：All,OnSourceExists
	 */
	private String inspectDifferenceMode;
	protected ProgressUpdate progressUpdateCallback;
	protected ConnectorNode sourceConnectorNode;
	protected ConnectorNode targetConnectorNode;
	protected ClientMongoOperator clientMongoOperator;

	public InspectContext(
			String name,
			InspectTask task,
			Connections source,
			Connections target,
			String inspectResultParentId,
			String inspectDifferenceMode,
			ProgressUpdate progressUpdateCallback,
			ConnectorNode sourceConnectorNode,
			ConnectorNode targetConnectorNode,
			ClientMongoOperator clientMongoOperator
	) {
		this.name = name;
		this.task = task;
		this.source = source;
		this.target = target;
		this.inspectResultParentId = inspectResultParentId;
		this.inspectDifferenceMode = inspectDifferenceMode;
		this.progressUpdateCallback = progressUpdateCallback;
		this.sourceConnectorNode = sourceConnectorNode;
		this.targetConnectorNode = targetConnectorNode;
		this.clientMongoOperator = clientMongoOperator;
	}

	public String getName() {
		return name;
	}

	public InspectTask getTask() {
		return task;
	}

	public Connections getSource() {
		return source;
	}

	public Connections getTarget() {
		return target;
	}

	public String getInspectResultParentId() {
		return inspectResultParentId;
	}

	public String getInspectDifferenceMode() {
		return inspectDifferenceMode;
	}

	public ProgressUpdate getProgressUpdateCallback() {
		return progressUpdateCallback;
	}

	public ConnectorNode getSourceConnectorNode() {
		return sourceConnectorNode;
	}

	public ConnectorNode getTargetConnectorNode() {
		return targetConnectorNode;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}
}
