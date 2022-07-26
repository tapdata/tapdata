/**
 * @title: DataFlowInsightTest
 * @description:
 * @author lk
 * @date 2022/2/9
 */
package com.tapdata.dataFlowInsight;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.TMApplication;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflowinsight.service.DataFlowInsightService;
import com.tapdata.tm.user.entity.User;
import java.util.Collections;
import java.util.HashMap;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AddFieldsOperation;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = {TMApplication.class})
@RunWith(SpringRunner.class)
public class DataFlowInsightTest {

	@Autowired
	MongoTemplate mongoTemplate;

	@Autowired
	DataFlowInsightService dataFlowInsightService;

	private String userId = "60cc0c5887e32100106b6a17";
//	private String userId = "61c295eae4c5aa06321f0b33";
//	private String userId = "6141da4566f51c001923f99e";

	/**
	 * db.DataFlowInsight.aggregate([
	 *
	 *     {$addFields: {
	 *       convertedId: { $toObjectId: "$dataFlowId" },
	 *       newStatsTime: { $toLong: "$statsTime" }
	 *    }},
	 *    {
	 *       $lookup:
	 *          {
	 *            from: "DataFlows",
	 *            let: { d_dataFlowId: "$convertedId" },
	 *            pipeline: [
	 *               { $match:
	 *                  { $expr:
	 *                     { $and:
	 *                        [
	 *                          { $eq: [ "$_id",  "$$d_dataFlowId" ] }
	 *                        ]
	 *                     }
	 *                  }
	 *               },
	 *               { $project: { user_id: 1, _id: 0 } }
	 *            ],
	 *            as: "newData"
	 *          }
	 *     },
	 *     {
	 *      $match:{"newData.user_id":"61416c9dc4e5c40012665933",granularity:"day",newStatsTime:{$gte:20220119000000}}
	 *         },{$sort:{"createTime":-1}},{$group:{_id:"$statsTime",statsData:{$first:"$statsData"},statsTime:{$first:"$statsTime"},granularity:{$first:"$granularity"},createTime:{$first:"$createTime"}}},
	 *         {$project:{t:{$sum:"$statsData.input.rows"}, statsTime:1,granularity:1,createTime:1}}
	 * ])
	 * ==========================
	 *
	 * db.DataFlowInsight.aggregate([
	 *
	 *     {$addFields: {
	 *       convertedId: { $toObjectId: "$dataFlowId" },
	 *       newStatsTime: { $toLong: "$statsTime" }
	 *    }},
	 *    {
	 *       $lookup:
	 *          {
	 *            from: "DataFlows",
	 *            localField: "convertedId",
	 *            foreignField: "_id",
	 *            as: "newData"
	 *          }
	 *     },
	 *     {
	 *      $match:{"newData.user_id":"61416c9dc4e5c40012665933",granularity:"day",newStatsTime:{$gte:20220119000000}}
	 *         },{$sort:{"createTime":-1}},{$group:{_id:"$statsTime",statsData:{$first:"$statsData"},statsTime:{$first:"$statsTime"},granularity:{$first:"$granularity"},createTime:{$first:"$createTime"}}},
	 *         {$project:{t:"$statsData.input.rows", statsTime:1,granularity:1,createTime:1}}
	 * ])
	 **/
	@Test
	public void testDataFlowInsight(){
		AddFieldsOperation addField = Aggregation.addFields().addFieldWithValueOf("newDataFlowId", new HashMap<String, String>(){{put("$toObjectId", "$dataFlowId");}})
				.addFieldWithValueOf("newStatsTime", new HashMap<String, String>(){{put("$toLong", "$statsTime");}}).build();
		LookupOperation lookup = Aggregation.lookup("DataFlows", "newDataFlowId", "_id", "newData");
		MatchOperation match = Aggregation.match(Criteria.where("newData.user_id").is(userId).and("granularity").is("day").and("newStatsTime").gte(20220119000000L));
		SortOperation sort = Aggregation.sort(Sort.by(Sort.Direction.DESC, "createTime"));
		GroupOperation group = Aggregation.group("$statsTime").first("$statsData").as("statsData")
				.first("$statsTime").as("statsTime")
				.first("$granularity").as("granularity")
				.first("$createTime").as("createTime");

		ProjectionOperation projection = Aggregation.project("statsTime", "granularity", "createTime").and("$statsData.input.rows").as("t");
		AggregationResults<Object> dataFlowInsight = mongoTemplate.aggregate(Aggregation.newAggregation(addField, lookup, match, sort, group, projection),
				"DataFlowInsight", Object.class);
		System.out.println("=====================================================================");
		System.out.println(dataFlowInsight.getMappedResults().size());
		dataFlowInsight.getMappedResults().stream().map(JsonUtil::toJson).forEach(System.out::println);
		System.out.println("=====================================================================");
	}

	/**
	 * db.DataFlowInsight.aggregate([
	 *
	 *     {$addFields: {
	 *       convertedId: { $toObjectId: "$dataFlowId" },
	 *       newStatsTime: { $toLong: "$statsTime" }
	 *    }},
	 *    {$match:{granularity:"day"}},
	 *    {
	 *       $lookup:
	 *          {
	 *            from: "DataFlows",
	 *            let: { d_dataFlowId: "$convertedId" },
	 *            pipeline: [
	 *               { $match:
	 *                  { $expr:
	 *                     { $and:
	 *                        [
	 *                          { $eq: [ "$_id",  "$$d_dataFlowId" ] }
	 *                        ]
	 *                     }
	 *                  }
	 *               },
	 *               { $project: { user_id: 1, _id: 0 } }
	 *            ],
	 *            as: "newData"
	 *          }
	 *     },
	 *     {
	 *      $match:{"newData.user_id":"619e04deb435c9383075741d"}
	 *         },{$sort:{"createTime":-1}},{$group:{_id:"$statsTime",statsData:{$first:"$statsData"},user_id:{$first:"$newData.user_id"}}},
	 *         {$group:{_id:"$user_id",count11:{$sum:"$statsData.input.rows"}}}
	 * ])
	 **/
	@Test
	public void totalInputDataCount(){

		AddFieldsOperation addField = Aggregation.addFields().addFieldWithValueOf("newDataFlowId", new HashMap<String, String>(){{put("$toObjectId", "$dataFlowId");}})
				.addFieldWithValueOf("newStatsTime", new HashMap<String, String>(){{put("$toLong", "$statsTime");}}).build();
		LookupOperation lookup = Aggregation.lookup("DataFlows", "newDataFlowId", "_id", "newData");
		MatchOperation match2 = Aggregation.match(Criteria.where("newData.user_id").is(userId).and("granularity").is("day"));
		SortOperation sort2 = Aggregation.sort(Sort.by(Sort.Direction.DESC, "createTime"));
		GroupOperation group2 = Aggregation.group("$statsTime").first("$statsData").as("statsData");
		GroupOperation group3 = Aggregation.group().sum("statsData.input.rows").as("count");
		AggregationResults<Object> dataFlowInsight2 = mongoTemplate.aggregate(Aggregation.newAggregation(addField, lookup, match2, /*sort2,*/ group2, group3),
				"DataFlowInsight", Object.class);
		System.out.println("=====================================================================");
		System.out.println(dataFlowInsight2.getMappedResults().size());
		System.out.println(JsonUtil.toJson(dataFlowInsight2.getMappedResults().get(0)));
		System.out.println("=====================================================================");

	}

	@Test
	public void testStatistics(){
		User user = new User();
		user.setId(new ObjectId(userId));
		Object statistics = dataFlowInsightService.statistics("day", new UserDetail(user, Collections.singleton(new SimpleGrantedAuthority("USERS"))));
		System.out.println("=====================================================================");
		System.out.println(JsonUtil.toJsonUseJackson(statistics));
		System.out.println("=====================================================================");
		Object statistics2 = dataFlowInsightService.statistics("week", new UserDetail(user, Collections.singleton(new SimpleGrantedAuthority("USERS"))));
		System.out.println("=====================================================================");
		System.out.println(JsonUtil.toJsonUseJackson(statistics2));
		System.out.println("=====================================================================");
		Object statistics3 = dataFlowInsightService.statistics("month", new UserDetail(user, Collections.singleton(new SimpleGrantedAuthority("USERS"))));
		System.out.println("=====================================================================");
		System.out.println(JsonUtil.toJsonUseJackson(statistics3));
		System.out.println("=====================================================================");
	}
}
