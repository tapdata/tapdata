package io.tapdata.milestone;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Job;
import com.tapdata.entity.Milestone;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-12-25 14:47
 **/
public class MilestoneFlowService extends MilestoneService {

	private final static Logger logger = LogManager.getLogger(MilestoneFlowService.class);

	private Map<String, MilestoneJobService> jobServiceMap;

	public MilestoneFlowService(MilestoneContext milestoneContext) {
		super(milestoneContext);

		initJobServiceMap();
	}

	/**
	 * 初始化所有子任务的里程碑服务，然后缓存到{@link MilestoneFlowService#jobServiceMap}
	 */
	private void initJobServiceMap() {
		ClientMongoOperator clientMongoOperator = this.milestoneContext.getClientMongoOperator();

		Query query = new Query(Criteria.where("dataFlowId").is(this.milestoneContext.getDataFlow().getId()));

		List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);

		jobServiceMap = new HashMap<>();
		for (Job job : jobs) {
			jobServiceMap.put(job.getId(), MilestoneFactory.getJobMilestoneService(job, clientMongoOperator));
		}
	}

	public void updateJobsMilestone(MilestoneStage milestoneStage, MilestoneStatus milestoneStatus, List<Job> jobs) {
		updateJobsMilestone(milestoneStage, milestoneStatus, "", jobs);
	}

	public void updateJobsMilestone(MilestoneStage milestoneStage, MilestoneStatus milestoneStatus, String errorMessage, List<Job> jobs) {
		if (!noneNull(milestoneStage, milestoneStatus) || CollectionUtils.isEmpty(jobs)) {
			return;
		}

		for (Job job : jobs) {
			if (jobServiceMap.containsKey(job.getId())) {
				jobServiceMap.get(job.getId()).updateMilestoneStatusByCode(milestoneStage, milestoneStatus, errorMessage);
			}
		}
	}

	/**
	 * 刷新任务编排的里程碑列表
	 */
	@Override
	public void updateList() {
		List<Milestone> milestones = mergeAndGetMilestones();
		updateList(milestones);
	}

	/**
	 * 将所有子任务的里程碑合并，获取任务编排的里程碑
	 *
	 * @return
	 */
	public List<Milestone> mergeAndGetMilestones() {
		if (null == this.milestoneContext.getDataFlow() || StringUtils.isBlank(this.milestoneContext.getDataFlow().getId())) {
			throw new IllegalArgumentException("Milestone context missing property: dataflow, dataflow.id");
		}

		ClientMongoOperator clientMongoOperator = this.milestoneContext.getClientMongoOperator();
		Map<String, Milestone> milestoneMap = new HashMap<>();

		Query query = new Query(Criteria.where("dataFlowId").is(this.milestoneContext.getDataFlow().getId()));
		query.fields().include(MILESTONES_FIELD_NAME);

		List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);

		if (CollectionUtils.isNotEmpty(jobs)) {
			for (Job job : jobs) {
				List<Milestone> jobMilestones = job.getMilestones();
				if (CollectionUtils.isEmpty(jobMilestones)) {
					continue;
				}

				for (Milestone jobMilestone : jobMilestones) {
					String code = jobMilestone.getCode();
					if (milestoneMap.containsKey(code)) {
						Milestone milestone = milestoneMap.get(code);

						// 如果有子任务的状态是running，并且原先的状态不是错误，则覆盖
						if (milestone.getStatus().equals(MilestoneStatus.FINISH.getStatus()) && jobMilestone.getStatus().equals(MilestoneStatus.RUNNING.getStatus())
								&& !milestone.getStatus().equals(MilestoneStatus.ERROR.getStatus())) {
							milestone.setStatus(jobMilestone.getStatus());
						}

						// 如果子任务状态是错误，则覆盖
						if (jobMilestone.getStatus().equals(MilestoneStatus.ERROR.getStatus())) {
							milestone.setStatus(jobMilestone.getStatus());
							milestone.setErrorMessage(milestone.getErrorMessage() + "\n\n" + jobMilestone.getErrorMessage());
						}

						// 如果子任务开始时间小于现有的开始时间，则覆盖
						if (((milestone.getStart() == null || milestone.getStart().compareTo(0L) <= 0) && jobMilestone.getStart() != null)
								|| (noneNull(milestone.getStart(), jobMilestone.getStart()) && jobMilestone.getStart().compareTo(milestone.getStart()) < 0)) {
							milestone.setStart(jobMilestone.getStart());
						}

						// 如果子任务的结束时间大于现有的结束时间，则覆盖
						if (((milestone.getEnd() == null || milestone.getEnd().compareTo(0L) <= 0) && jobMilestone.getEnd() != null)
								|| (noneNull(milestone.getEnd(), jobMilestone.getEnd()) && jobMilestone.getEnd().compareTo(milestone.getEnd()) > 0)) {
							milestone.setEnd(jobMilestone.getEnd());
						}
					} else {
						jobMilestone.setErrorMessage(jobMilestone.getErrorMessage() == null ? "" : jobMilestone.getErrorMessage());

						milestoneMap.put(jobMilestone.getCode(), jobMilestone);
					}
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

	@Override
	public List<Milestone> initMilestones() {

		jobServiceMap.forEach((jobId, milestoneJobService) -> {
			try {
				milestoneJobService.updateList(milestoneJobService.initMilestones());
			} catch (Exception e) {
				logger.warn("Init job milestone failed, id: {}, name: {}, err: {}", jobId, milestoneJobService.milestoneContext.getJob().getName(), e.getMessage(), e);
			}
		});
		List<Milestone> milestones = mergeAndGetMilestones();
		return milestones;
	}
}
