package com.tapdata.constant;

import com.tapdata.cache.ICacheService;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.User;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.Processor;
import io.tapdata.milestone.MilestoneService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tapdata on 08/12/2017.
 */
public class ConnectorContext implements Serializable {

	private static final long serialVersionUID = 4426488202338033615L;

	private Logger logger = LogManager.getLogger(ConnectorContext.class);

	/**
	 * job 信息
	 */
	private Job job;

	/**
	 * job connection
	 */
	private Connections jobSourceConn;
	private Connections jobTargetConn;

	/**
	 * tapdata 中间mongo 连接操作
	 */
	private ClientMongoOperator clientMongoOpertor;

	private List<Processor> processors = new ArrayList<>(0);

	private ICacheService cacheService;

	private Map<String, Map<String, Integer>> collectionsProjection;

	private boolean isCloud;

	private User user;

	private MilestoneService milestoneService;

	private ConfigurationCenter configurationCenter;

	public ConnectorContext() {
	}

	public ConnectorContext(Job job) {
		this.job = job;
	}

	public ConnectorContext(Job job, Connections jobSourceConn, ClientMongoOperator clientMongoOpertor,
							Connections jobTargetConn, List<Processor> processors, ICacheService cacheService,
							boolean isCloud, ConfigurationCenter configurationCenter) {
		this.job = job;
		this.jobSourceConn = jobSourceConn;
		this.jobTargetConn = jobTargetConn;
		this.clientMongoOpertor = clientMongoOpertor;
		this.processors = processors;
		this.cacheService = cacheService;

		List<Mapping> mappings = job.getMappings();

		this.collectionsProjection = getCollectionProjection(mappings);
		this.isCloud = isCloud;
		this.configurationCenter = configurationCenter;
		initUser();
	}

	public ConnectorContext(Job job, Connections jobSourceConn, ClientMongoOperator clientMongoOpertor,
							Connections jobTargetConn, List<Processor> processors, ICacheService cacheService, ConfigurationCenter configurationCenter) {
		this.job = job;
		this.jobSourceConn = jobSourceConn;
		this.jobTargetConn = jobTargetConn;
		this.clientMongoOpertor = clientMongoOpertor;
		this.processors = processors;
		this.cacheService = cacheService;

		List<Mapping> mappings = job.getMappings();

		this.collectionsProjection = getCollectionProjection(mappings);
		this.configurationCenter = configurationCenter;
		initUser();
	}

	public ConnectorContext(Job job, Connections jobSourceConn, ClientMongoOperator clientMongoOpertor,
							Connections jobTargetConn, List<Processor> processors, ICacheService cacheService,
							MilestoneService milestoneService, ConfigurationCenter configurationCenter) {
		this.job = job;
		this.jobSourceConn = jobSourceConn;
		this.jobTargetConn = jobTargetConn;
		this.clientMongoOpertor = clientMongoOpertor;
		this.processors = processors;
		this.cacheService = cacheService;
		this.milestoneService = milestoneService;

		List<Mapping> mappings = job.getMappings();

		this.collectionsProjection = getCollectionProjection(mappings);
		this.configurationCenter = configurationCenter;
		initUser();
	}

	public void initUser() {
		try {
			if (this.job != null && StringUtils.isNotBlank(this.job.getUser_id()) && this.clientMongoOpertor != null) {
				User user = this.clientMongoOpertor.findOne(new Query(Criteria.where("id").is(this.job.getUser_id())),
						"users/findOne", User.class);
				this.user = user;
			}
		} catch (Exception e) {
			logger.warn("Get job operator user error: {}, user id: {}, stacks: {}",
					e.getMessage(), job.getUser_id(), Log4jUtil.getStackString(e));
		}
	}

	public static Map<String, Map<String, Integer>> getCollectionProjection(List<Mapping> mappings) {
		Map<String, Map<String, Integer>> collectionsProjection = null;
		if (CollectionUtils.isNotEmpty(mappings)) {
			for (Mapping mapping : mappings) {
				String fieldFilterType = mapping.getFieldFilterType();
				String fieldFilter = mapping.getFieldFilter();
				Map<String, Integer> projection = new HashMap<>();
				if (Mapping.FIELD_FILTER_TYPE_RETAINED_FIELD.equals(fieldFilterType) && StringUtils.isNotBlank(fieldFilter)) {

					if (collectionsProjection == null) {
						collectionsProjection = new HashMap<>();
					}

					List<String> fields = Arrays.asList(fieldFilter.split(","));
					if (!fields.contains("_id")) {
						projection.put("_id", 0);
					}
					fields.forEach(field -> projection.put(field, 1));
					collectionsProjection.put(mapping.getFrom_table(), projection);

				} else if (Mapping.FIELD_FILTER_TYPE_DELETE_FIELD.equals(fieldFilterType) && StringUtils.isNotBlank(fieldFilter)) {

					if (collectionsProjection == null) {
						collectionsProjection = new HashMap<>();
					}

					List<String> fields = Arrays.asList(fieldFilter.split(","));

					fields.forEach(field -> projection.put(field, 0));

					collectionsProjection.put(mapping.getFrom_table(), projection);
				}
			}
		}

		return collectionsProjection;
	}

//    public ConnectorContext(Connections jobTargetConn, List<Processor> processors) {
//        this.jobTargetConn = jobTargetConn;
//        this.processors = processors;
//    }
//
//    public ConnectorContext(Job job, Connections jobSourceConn, Connections jobTargetConn) {
//        this.job = job;
//        this.jobSourceConn = jobSourceConn;
//        this.jobTargetConn = jobTargetConn;
//        this.startupTime = System.currentTimeMillis();
//    }
//
//    public ConnectorContext(Job job, Connections jobSourceConn, List<Processor> processors) {
//        this.job = job;
//        this.jobSourceConn = jobSourceConn;
//        this.processors = processors;
//        this.startupTime = System.currentTimeMillis();
//    }

	public boolean isRunning() {
		return ConnectorConstant.RUNNING.equals(job.getStatus())
				&& !Thread.currentThread().isInterrupted();
	}

	public Map<String, Map<String, Integer>> getCollectionsProjection() {
		return collectionsProjection;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public Connections getJobSourceConn() {
		return jobSourceConn;
	}

	public void setJobSourceConn(Connections jobSourceConn) {
		this.jobSourceConn = jobSourceConn;
	}

	public ClientMongoOperator getClientMongoOpertor() {
		return clientMongoOpertor;
	}

	public void setClientMongoOpertor(ClientMongoOperator clientMongoOpertor) {
		this.clientMongoOpertor = clientMongoOpertor;
	}

	public void setJobTargetConn(Connections jobTargetConn) {
		this.jobTargetConn = jobTargetConn;
	}

	public Connections getJobTargetConn() {
		return jobTargetConn;
	}

	public List<Processor> getProcessors() {
		return processors;
	}

	public void setProcessors(List<Processor> processors) {
		this.processors = processors;
	}

	public ICacheService getCacheService() {
		return cacheService;
	}

	public boolean isCloud() {
		return isCloud;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public MilestoneService getMilestoneService() {
		return milestoneService;
	}

	public void setMilestoneService(MilestoneService milestoneService) {
		this.milestoneService = milestoneService;
	}

	public ConfigurationCenter getConfigurationCenter() {
		return configurationCenter;
	}
}
