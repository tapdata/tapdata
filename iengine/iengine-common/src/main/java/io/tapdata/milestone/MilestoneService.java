package io.tapdata.milestone;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Milestone;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description 里程碑，处理业务逻辑
 * @create 2020-12-23 19:43
 **/
public abstract class MilestoneService implements Serializable {

	private final static Logger logger = LogManager.getLogger(MilestoneService.class);

	public static final String MILESTONES_FIELD_NAME = "milestones";
	private static final long serialVersionUID = 6863620611419564604L;
	protected MilestoneContext milestoneContext;

	public MilestoneService() {
	}

	protected MilestoneService(MilestoneContext milestoneContext) {
		this.milestoneContext = milestoneContext;
	}

	protected <T, E> boolean arrayNotInclude(E[] arr, T condition) {
		return null == Arrays.stream(arr).filter(d -> d.equals(condition)).findFirst().orElse(null);
	}

	protected <T, E> boolean arrayAllNotInclude(E[] arr, T... conditions) {
		for (T condition : conditions) {
			if (arrayInclude(arr, condition)) {
				return false;
			}
		}
		return true;
	}

	protected <T, E> boolean arrayInclude(E[] arr, T condition) {
		return null != Arrays.stream(arr).filter(d -> d.equals(condition)).findFirst().orElse(null);
	}

	protected <T, E> boolean arrayAllInclude(E[] arr, T... conditions) {
		for (T condition : conditions) {
			if (arrayNotInclude(arr, condition)) {
				return false;
			}
		}
		return true;
	}

	protected <T> T findObjectInList(List<T> list, Predicate predicate) {
		if (CollectionUtils.isEmpty(list)) {
			return null;
		}

		return list.stream().filter(t -> predicate.test(t)).findFirst().orElse(null);
	}

	public void updateMilestoneStatusByCode(MilestoneStage milestoneStage, MilestoneStatus milestoneStatus, String errorMessage) {
		try {
			if (null == milestoneStage || null == milestoneStatus) {
				throw new IllegalArgumentException("Input parameter is invalid, milestoneStage or milestoneStatus is null");
			}

			ClientMongoOperator clientMongoOperator = this.milestoneContext.getClientMongoOperator();
			if (clientMongoOperator == null) {
				clientMongoOperator = buildOperator();
			}
			String id;
			String collectionName;
			Query query = null;
			Update update = null;
			switch (milestoneContext.getMilestoneType()) {
				case JOB:
					if (null == this.milestoneContext.getJob()) {
						throw new RuntimeException("Milestone context missing property: job");
					}
					id = this.milestoneContext.getJob().getId();
					collectionName = ConnectorConstant.JOB_COLLECTION;
					query = new Query(Criteria.where("_id").is(id)
							.and("milestones").elemMatch(
									Criteria.where("code").is(milestoneStage.name()).and("status").ne(MilestoneStatus.ERROR.getStatus())
							)
					);
					update = new Update().set(MILESTONES_FIELD_NAME + ".$.status", milestoneStatus.getStatus());
					break;
				case DATAFLOW_V1:
				case DATAFLOW_V2:
					if (null == this.milestoneContext.getDataFlow()) {
						throw new RuntimeException("Milestone context missing property: dataflow");
					}
					id = this.milestoneContext.getDataFlow().getId();
					collectionName = ConnectorConstant.DATA_FLOW_COLLECTION;
					query = new Query(Criteria.where("_id").is(id)
							.and("milestones").elemMatch(
									Criteria.where("code").is(milestoneStage.name()).and("status").ne(MilestoneStatus.ERROR.getStatus())
							)
					);
					update = new Update().set(MILESTONES_FIELD_NAME + ".$.status", milestoneStatus.getStatus());
					break;
				case DATAFLOW_EDGE:
					if (null == this.milestoneContext.getDataFlow() || null == this.milestoneContext.getSourceStage() || null == this.milestoneContext.getDestStage()) {
						throw new RuntimeException("Milestone context missing property: dataflow, source stage, dest stage");
					}
					id = this.milestoneContext.getDataFlow().getId();
					collectionName = ConnectorConstant.DATA_FLOW_COLLECTION;
					String edgeKey = "edgeMilestones." + this.milestoneContext.getSourceStage().getVertexName() + ":" + this.milestoneContext.getDestStage().getVertexName() + "." + MILESTONES_FIELD_NAME;
					query = new Query(Criteria.where("_id").is(id)
							.and(edgeKey).elemMatch(
									Criteria.where("code").is(milestoneStage.name()).and("status").ne(MilestoneStatus.ERROR.getStatus())
							)
					);
					update = new Update().set(edgeKey + ".$.status", milestoneStatus.getStatus());
					break;
				default:
					id = "";
					collectionName = "";
					break;
			}

			if (StringUtils.isAnyBlank(id, collectionName)) {
				throw new RuntimeException("Update milestone stage failed, id or collection name is empty");
			}

			switch (milestoneContext.getMilestoneType()) {
				case DATAFLOW_EDGE:
					String edgeKey = "edgeMilestones." + this.milestoneContext.getSourceStage().getVertexName() + ":" + this.milestoneContext.getDestStage().getVertexName() + "." + MILESTONES_FIELD_NAME;
					// 开始时间
					if (milestoneStatus.equals(MilestoneStatus.RUNNING)) {
						update.set(edgeKey + ".$.start", System.currentTimeMillis());
					}
					// 结束时间
					if (milestoneStatus.equals(MilestoneStatus.FINISH) || milestoneStatus.equals(MilestoneStatus.ERROR)) {
						update.set(edgeKey + ".$.end", System.currentTimeMillis());
					}
					// 错误描述
					if (milestoneStatus.equals(MilestoneStatus.ERROR) && StringUtils.isNotBlank(errorMessage)) {
						update.set(edgeKey + ".$.errorMessage", errorMessage);
					}
					break;
				default:
					// 开始时间
					if (milestoneStatus.equals(MilestoneStatus.RUNNING)) {
						update.set(MILESTONES_FIELD_NAME + ".$.start", System.currentTimeMillis());
					}
					// 结束时间
					if (milestoneStatus.equals(MilestoneStatus.FINISH) || milestoneStatus.equals(MilestoneStatus.ERROR)) {
						update.set(MILESTONES_FIELD_NAME + ".$.end", System.currentTimeMillis());
					}
					// 错误描述
					if (milestoneStatus.equals(MilestoneStatus.ERROR) && StringUtils.isNotBlank(errorMessage)) {
						update.set(MILESTONES_FIELD_NAME + ".$.errorMessage", errorMessage);
					}
					break;
			}

			clientMongoOperator.update(query, update, collectionName);
		} catch (Exception e) {
			logger.warn("Change {} milestone stage {} status failed, status to {}, id: {}, name: {}, err: {}, stacks: {}",
					milestoneContext.getMilestoneType().name(), milestoneStage.name(), milestoneStatus.getStatus(),
					milestoneContext.getMilestoneType().equals(MilestoneContext.MilestoneType.JOB) ? milestoneContext.getJob().getId() : milestoneContext.getDataFlow().getId(),
					milestoneContext.getMilestoneType().equals(MilestoneContext.MilestoneType.JOB) ? milestoneContext.getJob().getName() : milestoneContext.getDataFlow().getName(),
					e.getMessage(), Log4jUtil.getStackString(e));
		}
	}

	protected boolean noneNull(Object... objects) {
		if (objects == null || objects.length <= 0) {
			return true;
		}

		for (Object object : objects) {
			if (object == null) {
				return false;
			}
		}

		return true;
	}

	public void updateMilestoneStatusByCode(MilestoneStage milestoneStage, MilestoneStatus milestoneStatus) {
		updateMilestoneStatusByCode(milestoneStage, milestoneStatus, "");
	}

	public void updateList(List<Milestone> milestoneList) {
		Query query;
		Update update;
		String resource;
		switch (milestoneContext.getMilestoneType()) {
			case JOB:
				query = new Query(Criteria.where("_id").is(milestoneContext.getJob().getId()));
				update = new Update().set(MILESTONES_FIELD_NAME, milestoneList);
				resource = ConnectorConstant.JOB_COLLECTION;
				break;
			case DATAFLOW_V1:
			case DATAFLOW_V2:
				query = new Query(Criteria.where("_id").is(milestoneContext.getDataFlow().getId()));
				update = new Update().set(MILESTONES_FIELD_NAME, milestoneList);
				resource = ConnectorConstant.DATA_FLOW_COLLECTION;
				break;
			default:
				return;
		}

		milestoneContext.getClientMongoOperator().update(query, update, resource);
	}

	public void updateList() {
		throw new UnsupportedOperationException();
	}

	abstract public List<Milestone> initMilestones();

	protected HttpClientMongoOperator buildOperator() {
		return new HttpClientMongoOperator(
				null, null,
				new RestTemplateOperator(milestoneContext.getBaseUrls(), milestoneContext.getRetryTime()),
				milestoneContext.getConfigurationCenter()
		);
	}

	public MilestoneContext getMilestoneContext() {
		return milestoneContext;
	}
}
