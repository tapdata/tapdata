package io.tapdata.flow.engine.manager;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2021-07-19 19:32
 **/
public class BaseManager {

	protected void refreshDataFlow(DataFlow dataFlow, ClientMongoOperator clientMongoOperator) {
		String dataFlowId = dataFlow.getId();
		Query query = new Query(where("_id").is(dataFlowId));
		query.fields().include("status").include("executeMode").include("id");

		List<DataFlow> dataFlows = clientMongoOperator.find(query, ConnectorConstant.DATA_FLOW_COLLECTION, DataFlow.class);
		if (CollectionUtils.isNotEmpty(dataFlows)) {
			for (DataFlow dbDataFlow : dataFlows) {
				dataFlow.setStatus(dbDataFlow.getStatus());
				if (!dbDataFlow.getExecuteMode().equals(dataFlow.getExecuteMode())) {
					clientMongoOperator.update(new Query(where("dataFlowId").is(dataFlowId)), new Update().set("executeMode", dbDataFlow.getExecuteMode()), ConnectorConstant.JOB_COLLECTION);
				}
				dataFlow.setExecuteMode(dbDataFlow.getExecuteMode());
			}
		}
	}
}
