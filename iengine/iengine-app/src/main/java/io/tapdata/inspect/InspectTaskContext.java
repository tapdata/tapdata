package io.tapdata.inspect;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectTask;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.pdk.core.api.ConnectorNode;

/**
 * @author samuel
 * @Description
 * @create 2022-06-06 11:49
 **/
public class InspectTaskContext extends InspectContext {

	public InspectTaskContext(
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
		super(name, task, source, target, inspectResultParentId, inspectDifferenceMode, progressUpdateCallback, sourceConnectorNode, targetConnectorNode, clientMongoOperator);
	}
}
