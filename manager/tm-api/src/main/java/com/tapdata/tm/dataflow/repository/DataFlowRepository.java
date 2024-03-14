/**
 * @title: DataFlowRepository
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.entity.DataFlow;
import java.util.Map;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

@Repository
public class DataFlowRepository extends BaseRepository<DataFlow, ObjectId> {
	public DataFlowRepository(MongoTemplate mongoOperations) {
		super(DataFlow.class, mongoOperations);
	}

	public UpdateResult updateOne(Query query, Map<String, Object> map){
		Update update = new Update();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			update.set(entry.getKey(), entry.getValue());
		}

		return mongoOperations.updateFirst(query, update, entityInformation.getJavaType());
	}

	public Map<String, Object> save(Map<String, Object> map, UserDetail userDetail){
		map.put("customId", userDetail.getCustomerId());
		map.put("user_id", userDetail.getUserId());
		return mongoOperations.save(map, entityInformation.getCollectionName());
	}
}
