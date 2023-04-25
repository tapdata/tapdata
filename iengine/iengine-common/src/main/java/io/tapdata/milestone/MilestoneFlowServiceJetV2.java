package io.tapdata.milestone;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Milestone;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2021-07-26 14:28
 **/
public class MilestoneFlowServiceJetV2 extends MilestoneService {

	private ClientMongoOperator clientMongoOperator;

	public MilestoneFlowServiceJetV2(MilestoneContext milestoneContext) {
		super(milestoneContext);
		TaskDto taskDto = milestoneContext.getTaskDto();
		Map<String, Object> attrs = taskDto.getAttrs();
		Object edgeMilestones = null;
		if (MapUtils.isNotEmpty(attrs)) {
			edgeMilestones = taskDto.getAttrs().get("edgeMilestones");
		}
		if (edgeMilestones instanceof Map && MapUtils.isNotEmpty((Map<?, ?>) edgeMilestones)) {
			Map<String, EdgeMilestone> newEdgeMilestones = new ConcurrentHashMap<>();
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) edgeMilestones).entrySet()) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				if (value instanceof Map) {
					EdgeMilestone edgeMilestone = JSONUtil.map2POJO((Map<?, ?>) value, new TypeReference<EdgeMilestone>() {{
					}});
					newEdgeMilestones.put(String.valueOf(key), edgeMilestone);
				}
			}
			milestoneContext.setEdgeMilestones(newEdgeMilestones);
		} else {
			milestoneContext.setEdgeMilestones(new ConcurrentHashMap<>());
		}
		this.clientMongoOperator = buildOperator();
	}

	@Override
	public List<Milestone> initMilestones() {
		List<Milestone> milestones = mergeAndGetMilestones();
		return milestones;
	}

	/**
	 * 将所有子任务的里程碑合并，获取任务编排的里程碑
	 *
	 * @return
	 */
	public List<Milestone> mergeAndGetMilestones() {
		if (null == this.milestoneContext.getTaskDto() || StringUtils.isBlank(this.milestoneContext.getTaskDto().getId().toHexString())) {
			throw new IllegalArgumentException("Milestone context missing property: dataflow, dataflow.id");
		}

		Map<String, Milestone> milestoneMap = new HashMap<>();
//    SubTaskDto subTaskDto = milestoneContext.getSubTaskDto();
		Map<String, EdgeMilestone> edgeMilestoneMap = this.milestoneContext.getEdgeMilestones();
		if (MapUtils.isEmpty(edgeMilestoneMap)) {
			return new ArrayList<>();
		}
		Iterator<String> iterator = edgeMilestoneMap.keySet().iterator();
		while (iterator.hasNext()) {
			String srcVertexNameAndTrgVertexName = iterator.next();
			List<Milestone> edgeMilestones = edgeMilestoneMap.get(srcVertexNameAndTrgVertexName).getMilestones();
			for (Milestone edgeMilestone : edgeMilestones) {
				String code = edgeMilestone.getCode();
				if (milestoneMap.containsKey(code)) {
					Milestone milestone = milestoneMap.get(code);

					// 如果有子任务的状态是running，则覆盖
					if (milestone.getStatus().equals(MilestoneStatus.FINISH.getStatus()) && edgeMilestone.getStatus().equals(MilestoneStatus.RUNNING.getStatus())) {
						milestone.setStatus(edgeMilestone.getStatus());
					}

					// 如果子任务开始时间小于现有的开始时间，则覆盖
					if (((milestone.getStart() == null || milestone.getStart().compareTo(0L) <= 0) && edgeMilestone.getStart() != null)
							|| (noneNull(milestone.getStart(), edgeMilestone.getStart()) && edgeMilestone.getStart().compareTo(milestone.getStart()) < 0)) {
						milestone.setStart(edgeMilestone.getStart());
					}

					// 如果子任务的结束时间大于现有的结束时间，则覆盖
					if (((milestone.getEnd() == null || milestone.getEnd().compareTo(0L) <= 0) && edgeMilestone.getEnd() != null)
							|| (noneNull(milestone.getEnd(), edgeMilestone.getEnd()) && edgeMilestone.getEnd().compareTo(milestone.getEnd()) > 0)) {
						milestone.setEnd(edgeMilestone.getEnd());
					}

					milestone.setErrorMessage(milestone.getErrorMessage() + "\n\n" + edgeMilestone.getErrorMessage());
				} else {
					edgeMilestone.setErrorMessage(edgeMilestone.getErrorMessage() == null ? "" : edgeMilestone.getErrorMessage());

					milestoneMap.put(edgeMilestone.getCode(), edgeMilestone);
				}
			}
		}

		// 生成里程碑列表，需要保证里程碑节点顺序
		List<Milestone> milestones = new ArrayList<>();
		for (MilestoneStage milestoneStage : MilestoneStage.values()) {
			if (milestoneMap.containsKey(milestoneStage.name())) {
				milestones.add(milestoneMap.get(milestoneStage.name()));
			}
		}
		return milestones;
	}

	/**
	 * 将edge milestone list和flow milestone list都更新到SubTasks中
	 */
	@Override
	public void updateList() {

		// 从中间库拉取最新的edgeMilestones数据
		Query query = new Query(Criteria.where("_id").is(milestoneContext.getTaskDto().getId().toHexString()));
		query.fields().include("attrs.edgeMilestones");
		TaskDto taskDto = clientMongoOperator.findOne(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		Map<String, EdgeMilestone> edgeMilestoneMap = new ConcurrentHashMap<>();
		if (MapUtils.isNotEmpty(taskDto.getAttrs())) {
			final Map<String, Object> edgeMilestones = (Map) taskDto.getAttrs().getOrDefault("edgeMilestones", new ConcurrentHashMap<>());
			if (MapUtils.isNotEmpty(edgeMilestones)) {
				for (Map.Entry<String, Object> objectEntry : edgeMilestones.entrySet()) {
					final String key = objectEntry.getKey();
					final Map<String, Object> milestones = (Map) objectEntry.getValue();
					if (MapUtils.isNotEmpty(milestones)) {
						edgeMilestoneMap.put(key, JSONUtil.map2POJO(milestones, EdgeMilestone.class));
					}
				}
			}
			if (MapUtils.isNotEmpty(edgeMilestoneMap)) {
				milestoneContext.setEdgeMilestones(edgeMilestoneMap);
			}
		}

		List<com.tapdata.tm.commons.task.dto.Milestone> taskMilestones = new ArrayList<>();
		final List<Milestone> milestones = mergeAndGetMilestones();
		if (CollectionUtils.isNotEmpty(milestones)) {
			for (Milestone milestone : milestones) {
				com.tapdata.tm.commons.task.dto.Milestone taskMilestone = new com.tapdata.tm.commons.task.dto.Milestone();
				BeanUtils.copyProperties(milestone, taskMilestone);
				taskMilestones.add(taskMilestone);
			}
		}
		// 根据edgeMilestones合并出milestone list
		milestoneContext.getTaskDto().setMilestones(taskMilestones);
		query = new Query(Criteria.where("_id").is(milestoneContext.getTaskDto().getId().toHexString()));
		Update update = new Update().set(MILESTONES_FIELD_NAME, milestoneContext.getTaskDto().getMilestones());
//      .set("attrs.edgeMilestones", milestoneContext.getEdgeMilestones());
		clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
	}

	/**
	 * 将flow milestone list跟新到SubTasks中
	 *
	 * @param milestoneList
	 */
	@Override
	public void updateList(List<Milestone> milestoneList) {
		Query query = new Query(Criteria.where("_id").is(milestoneContext.getTaskDto().getId().toHexString()));
		Update update = new Update().set(MILESTONES_FIELD_NAME, milestoneList);
		clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
	}
}
