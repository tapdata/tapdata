package io.tapdata.dao;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.TapdataShareContext;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author tapdata
 * @date 02/04/2018
 */
@Component("messageDao")
public class MessageDao {

	private Logger logger = LogManager.getLogger(MessageDao.class);
	private Map<String, LinkedBlockingQueue<List<MessageEntity>>> jobMessageQueue = new ConcurrentHashMap<>();
	private Map<String, Job> runningJobCache = new ConcurrentHashMap<>();
	private Map<String, TapdataShareContext> tapdataShareContextMap = new ConcurrentHashMap<>();

	private LinkedBlockingQueue<String> runningJobs = new LinkedBlockingQueue<>();
	private LinkedBlockingQueue<String> stopJobs = new LinkedBlockingQueue<>();

	private Set<String> cacheRegisterJobIds = new HashSet<>();
	private ICacheService cacheService;

	public synchronized LinkedBlockingQueue<List<MessageEntity>> createJobMessageQueue(Job job) {
		String jobId = job.getId();
		LinkedBlockingQueue<List<MessageEntity>> msgQueue;
		if (!jobMessageQueue.containsKey(jobId)) {
			msgQueue = new LinkedBlockingQueue<>(20);
			jobMessageQueue.put(jobId, msgQueue);
		}

		return jobMessageQueue.get(jobId);
	}

	public synchronized Job getCacheJob(Job job) {
		String jobId = job.getId();
		if (!runningJobCache.containsKey(jobId) && ConnectorConstant.RUNNING.equals(job.getStatus())) {
			runningJobCache.put(jobId, job);
		}

		return runningJobCache.get(jobId);
	}

	public synchronized TapdataShareContext getCacheTapdataShareContext(Job job) {
		String jobId = job.getId();
		if (!tapdataShareContextMap.containsKey(jobId) && ConnectorConstant.RUNNING.equals(job.getStatus())) {
			tapdataShareContextMap.put(jobId, new TapdataShareContext());
		}

		return tapdataShareContextMap.get(jobId);
	}

	public void removeJobMessageQueue(String jobId) {
		jobMessageQueue.remove(jobId);
	}

	public void removeJobCache(String jobId) {
		runningJobCache.remove(jobId);
	}

	public void removeTapdataShareContext(String jobId) {
		tapdataShareContextMap.remove(jobId);
	}

	public LinkedBlockingQueue<String> getRunningJobsQueue() {
		return runningJobs;
	}

	public LinkedBlockingQueue<String> getStopJobs() {
		return stopJobs;
	}

	public ICacheService getCacheService() {
		return cacheService;
	}

	public void setCacheService(ICacheService memoryCacheService) {
		this.cacheService = memoryCacheService;
	}

	public synchronized void registerCache(Job job, ClientMongoOperator clientMongoOperator) {
		if (cacheRegisterJobIds.contains(job.getId())) {
			logger.info("Job cache is registered '{}'", job.getId());
			return;
		}
		cacheRegisterJobIds.add(job.getId());
		CacheUtil.registerCache(job, clientMongoOperator, cacheService);
	}

	public synchronized void registerCache(CacheNode cacheNode, TableNode sourceNode, Connections sourceConnection, TaskDto taskDto, ClientMongoOperator clientMongoOperator) {
		if (cacheRegisterJobIds.contains(taskDto.getId().toHexString())) {
			logger.info("Job cache is registered '{}'", taskDto.getId());
			return;
		}
		cacheRegisterJobIds.add(taskDto.getId().toHexString());
		CacheUtil.registerCache(cacheNode, sourceNode, sourceConnection, clientMongoOperator, cacheService);
	}

	public synchronized void destroyCache(Job job) {
		cacheRegisterJobIds.remove(job.getId());
		CacheUtil.destroyCache(job, cacheService);
	}

	public synchronized void destroyCache(TaskDto taskDto, String cacheName) {
		cacheRegisterJobIds.remove(taskDto.getId().toHexString());
		cacheService.destroy(cacheName);
	}

	public synchronized void updateCacheStatus(String cacheName, String status) {
		cacheService.updateCacheStatus(cacheName, status);
	}
}
