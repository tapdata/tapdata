package com.tapdata.tm.ds.service.impl;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.ILock;
import com.tapdata.tm.dblock.LockStateEnums;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.ds.dto.PdkVersionCheckDto;
import com.tapdata.tm.ds.repository.PdkSourceRepository;
import com.tapdata.tm.ds.vo.PdkFileTypeEnum;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.*;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2022/2/23
 * @Description: pdk相关的业务处理
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class PkdSourceService {

	public static final String METADATA_PDK_APIBUILD_NUMBER = "metadata.pdkAPIBuildNumber";
	public static final String METADATA_PDK_HASH = "metadata.pdkHash";
	@Setter(AccessLevel.NONE)
	@Value("${pdk.registration.task-stop-timeout-millis:60000}")
	private long taskStopTimeoutMillis = 60_000L;
	@Setter(AccessLevel.NONE)
	@Value("${pdk.registration.task-stop-poll-interval-millis:1000}")
	private long taskStopPollIntervalMillis = 1_000L;
	// Renew the registration lock well before its lease elapses so a long running registration
	// (stop tasks + wait + upload) cannot lose its lease to a concurrent registration.
	@Setter(AccessLevel.NONE)
	@Value("${pdk.registration.lock-renew-interval-millis:60000}")
	private long registrationLockRenewIntervalMillis = 60_000L;
	private static final long CONNECTOR_REGISTRATION_LOCK_TIMEOUT_MILLIS = 300_000L;
	private static final String CONNECTOR_REGISTRATION_LOCK_PREFIX = "PkdSourceService.uploadPdk.";
	private DataSourceDefinitionService dataSourceDefinitionService;
	private DataSourceService dataSourceService;
	private TaskService taskService;
	private InspectService inspectService;
	private DBLockRepository dbLockRepository;
	private DBLockConfiguration dbLockConfiguration;
	private FileService fileService;
	private TcmService tcmService;
	private SettingsService settingsService;
	private PdkSourceRepository repository;

	@PostConstruct
	private void validateRegistrationLockConfiguration() {
		if (registrationLockRenewIntervalMillis <= 0
				|| registrationLockRenewIntervalMillis >= CONNECTOR_REGISTRATION_LOCK_TIMEOUT_MILLIS) {
			throw new IllegalStateException("pdk.registration.lock-renew-interval-millis must be greater than 0 and less than "
					+ CONNECTOR_REGISTRATION_LOCK_TIMEOUT_MILLIS + " milliseconds");
		}
	}

	@SuppressWarnings(value = "unchecked")
	public void uploadPdk(MultipartFile[] files, List<PdkSourceDto> pdkSourceDtos, boolean latest, UserDetail user,
			boolean uploadOnlyRegistration) {
		Map<String, MultipartFile> iconMap = new HashMap<>();
		Map<String, MultipartFile> docMap = new HashMap<>();
		MultipartFile jarFile = null;
		for (MultipartFile multipartFile : files) {
			if (log.isDebugEnabled()) {
				log.debug("multipartFile name: {}, file size: {}", multipartFile.getOriginalFilename(), multipartFile.getSize());
			}
			if (multipartFile.getOriginalFilename() != null && multipartFile.getOriginalFilename().endsWith(".jar")) {
				jarFile = multipartFile;
			} else if (multipartFile.getOriginalFilename() != null && multipartFile.getOriginalFilename().endsWith(".md")) {
				docMap.put(multipartFile.getOriginalFilename(), multipartFile);
			} else {
				iconMap.put(multipartFile.getOriginalFilename(), multipartFile);
			}
		}

		if (jarFile == null) {
			throw new BizException("Invalid jar file, please upload a valid jar file.");
		}

		String lockOwner = dbLockConfiguration.getOwner() + ":" + UUID.randomUUID();
		List<ILock> registrationLocks = new CopyOnWriteArrayList<>();
		RegistrationLockRenewal lockRenewal = null;
		List<TaskDto> stoppedTasks = new ArrayList<>();
		List<InspectDto> stoppedInspects = new ArrayList<>();
		List<String> restartFailures = new ArrayList<>();
		RuntimeException registrationFailure = null;
		try {
			registrationLocks = acquireRegistrationLocks(pdkSourceDtos, lockOwner);
			lockRenewal = scheduleRegistrationLockRenewal(registrationLocks, lockOwner);
			if (uploadOnlyRegistration) {
				lockRenewal.ensureHealthy();
				uploadPdkDefinitions(jarFile, iconMap, docMap, pdkSourceDtos, latest, user);
				lockRenewal.ensureHealthy();
				return;
			}
			Set<String> connectionIds = findAffectedConnectionIds(pdkSourceDtos, user);
			// Query all matching tasks before deciding which ones to pause. A concurrent registration may
			// already have moved a task to STOP; filtering STOP here would let the second registration
			// skip the task lock and upload while the first registration still owns that task.
			lockRenewal.ensureHealthy();
			List<TaskDto> taskCandidates = findAffectedTasks(connectionIds, user);
			// Besides the per-connector locks acquired above, serialise registrations that touch the same
			// tasks. Two different connectors may be referenced by one task, so connector level locks alone
			// do not stop two registrations from stopping/restarting the same task concurrently.
			acquireAffectedTaskLocks(taskCandidates, lockOwner, registrationLocks);
			// The candidate status was read before the task locks were acquired. Read it again while
			// holding those locks so a task stopped and then restarted by a concurrent registration is
			// paused again before this registration uploads the new connector.
			List<TaskDto> affectedTasks = findCurrentAffectedTasks(taskCandidates, user);
			pauseAffectedTasks(affectedTasks, stoppedTasks, user);
			List<InspectDto> inspectCandidates = findAffectedInspects(connectionIds, user);
			acquireAffectedInspectLocks(inspectCandidates, lockOwner, registrationLocks);
			List<InspectDto> affectedInspects = findCurrentAffectedInspects(inspectCandidates, user);
			stopAffectedInspects(affectedInspects, stoppedInspects, user);
			waitForAffectedResourcesStopped(stoppedTasks, stoppedInspects, user, lockRenewal);
			lockRenewal.ensureHealthy();
			uploadPdkDefinitions(jarFile, iconMap, docMap, pdkSourceDtos, latest, user);
			lockRenewal.ensureHealthy();
		} catch (RuntimeException e) {
			registrationFailure = e;
		} finally {
			// A status poll may have set the interrupt flag before aborting registration. Clear it
			// during restoration, then restore the flag so callers still observe the interruption.
			boolean wasInterrupted = Thread.interrupted();
			try {
				restartAffectedInspects(stoppedInspects, user, restartFailures);
				restartAffectedTasks(stoppedTasks, user, restartFailures);
			} finally {
				if (lockRenewal != null) {
					lockRenewal.stop();
				}
				releaseRegistrationLocks(registrationLocks, lockOwner);
				if (wasInterrupted || Thread.interrupted()) {
					Thread.currentThread().interrupt();
				}
			}
		}
		// The affected resources are already stopped at this point. A failed restart must
		// never be swallowed, otherwise live business tasks would silently stay in STOP
		// after a connector registration.
		if (!restartFailures.isEmpty()) {
			BizException restartException = new BizException(
					"Connector registered but some affected resources could not be restarted: "
							+ String.join("; ", restartFailures));
			if (registrationFailure != null) {
				registrationFailure.addSuppressed(restartException);
				throw registrationFailure;
			}
			throw restartException;
		}
		if (registrationFailure != null) {
			throw registrationFailure;
		}
		log.debug("Upload pdk done.");
	}

	private List<ILock> acquireRegistrationLocks(List<PdkSourceDto> pdkSourceDtos, String lockOwner) {
		List<String> lockKeys = pdkSourceDtos.stream()
				.filter(Objects::nonNull)
				.filter(pdkSourceDto -> StringUtils.isNotBlank(pdkSourceDto.getId()))
				.map(pdkSourceDto -> CONNECTOR_REGISTRATION_LOCK_PREFIX + pdkSourceDto.getGroup() + "." + pdkSourceDto.getId())
				.distinct()
				.sorted()
				.collect(Collectors.toList());
		List<ILock> acquiredLocks = new CopyOnWriteArrayList<>();
		try {
			for (String lockKey : lockKeys) {
				acquireRegistrationLock(lockKey, lockOwner, acquiredLocks);
			}
			return acquiredLocks;
		} catch (RuntimeException e) {
			releaseRegistrationLocks(acquiredLocks, lockOwner);
			throw e;
		}
	}

	private void acquireRegistrationLock(String lockKey, String lockOwner, List<ILock> acquiredLocks) {
		ILock lock = DBLock.create(dbLockRepository, lockKey);
		LockStateEnums lockState = lock.acquire(lockOwner, CONNECTOR_REGISTRATION_LOCK_TIMEOUT_MILLIS);
		if (!lockState.isYes()) {
			throw new BizException("Connector registration is already in progress: " + lockKey);
		}
		acquiredLocks.add(lock);
	}

	/**
	 * Serialises registrations that stop/restart the same tasks. Two different connectors may be referenced
	 * by one task, so per-connector locks alone do not prevent two registrations from racing on that task.
	 * Any locks acquired here are appended to {@code acquiredLocks} and released by the caller.
	 */
	private void acquireAffectedTaskLocks(List<TaskDto> affectedTasks, String lockOwner, List<ILock> acquiredLocks) {
		List<String> lockKeys = affectedTasks.stream()
				.map(TaskDto::getId)
				.filter(Objects::nonNull)
				.distinct()
				.map(taskId -> CONNECTOR_REGISTRATION_LOCK_PREFIX + "task." + taskId.toHexString())
				.sorted()
				.collect(Collectors.toList());
		for (String lockKey : lockKeys) {
			acquireRegistrationLock(lockKey, lockOwner, acquiredLocks);
		}
	}

	private void acquireAffectedInspectLocks(List<InspectDto> affectedInspects, String lockOwner,
			List<ILock> acquiredLocks) {
		List<String> lockKeys = affectedInspects.stream()
				.map(InspectDto::getId)
				.filter(Objects::nonNull)
				.distinct()
				.map(inspectId -> CONNECTOR_REGISTRATION_LOCK_PREFIX + "inspect." + inspectId.toHexString())
				.sorted()
				.collect(Collectors.toList());
		for (String lockKey : lockKeys) {
			acquireRegistrationLock(lockKey, lockOwner, acquiredLocks);
		}
	}

	private RegistrationLockRenewal scheduleRegistrationLockRenewal(List<ILock> registrationLocks, String lockOwner) {
		validateRegistrationLockConfiguration();
		return new RegistrationLockRenewal(registrationLocks, lockOwner, registrationLockRenewIntervalMillis);
	}

	/**
	 * Keeps the connector registration locks alive while a registration is in progress. The locks are acquired
	 * with a fixed lease ({@link #CONNECTOR_REGISTRATION_LOCK_TIMEOUT_MILLIS}); without renewal a slow
	 * registration could out-live the lease and let a concurrent registration take over. {@link #stop()} must be
	 * called before the locks are released so a renewal cannot re-acquire a lock after it was released.
	 */
	private static final class RegistrationLockRenewal {
		private final List<ILock> locks;
		private final String owner;
		private final Object monitor = new Object();
		private final ScheduledFuture<?> future;
		private volatile boolean renewing = true;
		private final AtomicReference<RuntimeException> renewalFailure = new AtomicReference<>();

		private RegistrationLockRenewal(List<ILock> locks, String owner, long renewIntervalMillis) {
			this.locks = locks;
			this.owner = owner;
			this.future = DBLock.executor.scheduleWithFixedDelay(this::renewAll,
					renewIntervalMillis, renewIntervalMillis, TimeUnit.MILLISECONDS);
		}

		private void renewAll() {
			synchronized (monitor) {
				if (!renewing) {
					return;
				}
				for (ILock lock : locks) {
					try {
						LockStateEnums state = lock.acquire(owner, CONNECTOR_REGISTRATION_LOCK_TIMEOUT_MILLIS);
						if (!state.isYes()) {
							fail(new IllegalStateException("Lost connector registration lock '" + lock.getKey()
									+ "' for owner '" + owner + "'"));
							return;
						}
					} catch (Exception e) {
						fail(new IllegalStateException("Failed to renew connector registration lock '" + lock.getKey()
								+ "' for owner '" + owner + "'", e));
						return;
					}
				}
			}
		}

		private void fail(RuntimeException failure) {
			if (renewalFailure.compareAndSet(null, failure)) {
				log.error(failure.getMessage(), failure);
			}
			renewing = false;
		}

		private void ensureHealthy() {
			RuntimeException failure = renewalFailure.get();
			if (failure != null) {
				throw failure;
			}
		}

		private void stop() {
			future.cancel(false);
			synchronized (monitor) {
				// Wait for any in-flight renewal, so no lock is re-acquired after the caller releases it.
				renewing = false;
			}
		}
	}

	private void releaseRegistrationLocks(List<ILock> registrationLocks, String lockOwner) {
		ListIterator<ILock> iterator = registrationLocks.listIterator(registrationLocks.size());
		while (iterator.hasPrevious()) {
			ILock lock = iterator.previous();
			try {
				if (!lock.release(lockOwner)) {
					log.warn("Failed to release connector registration lock '{}' for owner '{}'", lock.getKey(), lockOwner);
				}
			} catch (Exception e) {
				log.warn("Failed to release connector registration lock '{}' for owner '{}'", lock.getKey(), lockOwner, e);
			}
		}
	}

	private void uploadPdkDefinitions(MultipartFile jarFile, Map<String, MultipartFile> iconMap,
			Map<String, MultipartFile> docMap, List<PdkSourceDto> pdkSourceDtos, boolean latest, UserDetail user) {
		Map<String, Object> oemConfig = OEMReplaceUtil.getOEMConfigMap("connector/replace.json");
		for(PdkSourceDto pdkSourceDto : pdkSourceDtos) {
            // try to verify the version
            String version  = pdkSourceDto.getVersion();
            Integer pdkAPIBuildNumber = pdkSourceDto.getPdkAPIBuildNumber();

			// 只有 admin 用户的为 public 的 scope
			String scope = "customer";
			//云版没有admin所以采用这种方式
			if ("admin@admin.com".equals(user.getEmail()) || "18973231732".equals(user.getUsername())) {
				scope = "public";
			}
			Criteria criteria = Criteria.where("scope").is(scope)
					.and("group").is(pdkSourceDto.getGroup())
					.and("version").is(version)
					.and("pdkAPIBuildNumber").is(pdkAPIBuildNumber)
					.and("pdkId").is(pdkSourceDto.getId())
					.and("is_deleted").is(false);
			if ("customer".equals(scope)) {
				criteria.and("customId").is(user.getCustomerId());
			}
			DataSourceDefinitionDto oldDefinitionDto = dataSourceDefinitionService.findOne(new Query(criteria));
			if (!version.endsWith("-SNAPSHOT") && oldDefinitionDto != null) {
				throw new BizException("Only SNAPSHOT version of PDK can be overwritten, please make sure you've updated the version in your pom.");
			}

			DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
			BeanUtils.copyProperties(pdkSourceDto, definitionDto);
			//definitionDto.setId(Objects.nonNull(oldDefinitionDto) ? oldDefinitionDto.getId() : null);
			definitionDto.setConnectionType(pdkSourceDto.getType());
			definitionDto.setType(pdkSourceDto.getName());
			definitionDto.setRealName(pdkSourceDto.getRealName());
			definitionDto.setPdkType("pdk");
			definitionDto.setPdkId(pdkSourceDto.getId());
			definitionDto.setJarFile(jarFile.getOriginalFilename());
			definitionDto.setJarTime(System.currentTimeMillis());
			definitionDto.setProperties(pdkSourceDto.getConfigOptions());
			definitionDto.setScope(scope);
			String pdkHash = definitionDto.calculatePdkHash(user.getCustomerId());
			definitionDto.setPdkHash(pdkHash);

			// remove snapshot overwritten file(jar/icons)
			if (oldDefinitionDto != null) {
				// change to async delete
				List<ObjectId> fileIds = new ArrayList<>();
				fileIds.add(MongoUtils.toObjectId(oldDefinitionDto.getJarRid()));
				if (oldDefinitionDto.getIcon() != null) {
					fileIds.add(MongoUtils.toObjectId(oldDefinitionDto.getIcon()));
				}
				Query query = Query.query(Criteria.where(METADATA_PDK_HASH).is(pdkHash)
						.and(METADATA_PDK_APIBUILD_NUMBER).is(pdkAPIBuildNumber));
				GridFSFindIterable result = fileService.find(query);
				result.forEach(gridFSFile ->
					fileIds.add(gridFSFile.getObjectId())
				);

				fileService.scheduledDeleteFiles(fileIds, "Upload new connector", "DatabaseTypes",
						oldDefinitionDto.getId(), user);
				log.debug("Delete original source {}", oldDefinitionDto.getId());
			}

			// upload the associated files(jar/icons)
			ObjectId jarObjectId = null;
			ObjectId iconObjectId = null;
			try {
				Map<String, Object> fileInfo = Maps.newHashMap();
				log.info("jarFile name : {} ,originalFilename : {}",jarFile.getName(),jarFile.getOriginalFilename());
				String md5 = PdkSourceUtils.getFileMD5(jarFile);
				fileInfo.put("pdkHash", pdkHash);
				fileInfo.put("pdkAPIBuildNumber", pdkAPIBuildNumber);
				fileInfo.put("md5", md5);

				// 1. upload jar file, only update once
				jarObjectId = fileService.storeFile(jarFile.getInputStream(), jarFile.getOriginalFilename(), null, fileInfo);

				// 2. upload the associated icon
				MultipartFile icon = iconMap.getOrDefault(pdkSourceDto.getIcon(), null);
				if (icon != null) {
					iconObjectId = fileService.storeFile(icon.getInputStream(), icon.getOriginalFilename(), null, fileInfo);
				}
				// 3. upload readeMe doc
                uploadDocs(docMap, pdkSourceDto.getMessages(), fileInfo, oemConfig);
                log.debug("Upload file to GridFS success");
			} catch (IOException e) {
				throw new BizException("SystemError", e);
			}

			definitionDto.setJarRid(jarObjectId.toHexString());
			if (iconObjectId != null) {
				definitionDto.setIcon(iconObjectId.toHexString());
			}

			if (latest) {
				definitionDto.setLatest(true);
				// set last latest to false
				Criteria criteriaLatest = Criteria.where("scope").is(scope)
						.and("pdkId").is(pdkSourceDto.getId())
						.and("group").is(pdkSourceDto.getGroup())
						.and("latest").is(true)
						.and("is_deleted").is(false);
				if ("customer".equals(scope)) {
					criteriaLatest.and("customId").is(user.getCustomerId());
				}
				Update removeLatest = Update.update("latest", false);
				dataSourceDefinitionService.update(new Query(criteriaLatest), removeLatest);
			}
			definitionDto.setId(null);
			if (Objects.isNull(oldDefinitionDto)) {
				dataSourceDefinitionService.save(definitionDto, user);
			} else {
				dataSourceDefinitionService.upsert(Query.query(Criteria.where("_id").is(oldDefinitionDto.getId())), definitionDto, user);
			}
			log.debug("Upsert data source definition success");
		}
	}

	private Set<String> findAffectedConnectionIds(List<PdkSourceDto> pdkSourceDtos, UserDetail user) {
		List<Criteria> connectorCriteria = pdkSourceDtos.stream()
				.filter(Objects::nonNull)
				.filter(pdkSourceDto -> StringUtils.isNotBlank(pdkSourceDto.getId()))
				.map(pdkSourceDto -> Criteria.where("definitionPdkId").is(pdkSourceDto.getId())
						.and("definitionGroup").is(pdkSourceDto.getGroup()))
				.collect(Collectors.toList());
		if (CollectionUtils.isEmpty(connectorCriteria)) {
			return Collections.emptySet();
		}

		Criteria criteria = new Criteria().andOperator(
				Criteria.where("is_deleted").ne(true),
				new Criteria().orOperator(connectorCriteria)
		);
		Query query = Query.query(criteria);
		query.fields().include("_id");
		List<DataSourceConnectionDto> connections = dataSourceService.findAllDto(query, user);
		if (CollectionUtils.isEmpty(connections)) {
			return Collections.emptySet();
		}
		return connections.stream()
				.map(DataSourceConnectionDto::getId)
				.filter(Objects::nonNull)
				.map(ObjectId::toHexString)
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	private List<TaskDto> findAffectedTasks(Set<String> connectionIds, UserDetail user) {
		if (CollectionUtils.isEmpty(connectionIds)) {
			return Collections.emptyList();
		}
		List<Criteria> connectionCriteria = new ArrayList<>();
		connectionCriteria.add(Criteria.where("dag.nodes.connectionId").in(connectionIds));
		connectionCriteria.add(Criteria.where("dag.nodes.connectionIds").in(connectionIds));
		connectionIds.forEach(connectionId -> connectionCriteria.add(
				Criteria.where("dag.nodes.logCollectorConnConfigs." + connectionId).exists(true)));

		Criteria criteria = new Criteria().andOperator(
				Criteria.where("is_deleted").ne(true),
				Criteria.where("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE,
						TaskDto.SYNC_TYPE_LOG_COLLECTOR, TaskDto.SYNC_TYPE_CONN_HEARTBEAT,
						TaskDto.SYNC_TYPE_MEM_CACHE),
				new Criteria().orOperator(connectionCriteria)
		);
		Query query = Query.query(criteria);
		query.fields().include("_id", "name", "status", "syncType");
		List<TaskDto> tasks = taskService.findAllDto(query, user);
		if (CollectionUtils.isEmpty(tasks)) {
			return Collections.emptyList();
		}

		tasks.sort(Comparator.comparingInt(this::taskStopOrder));
		return tasks;
	}

	private List<TaskDto> findCurrentAffectedTasks(List<TaskDto> taskCandidates, UserDetail user) {
		Set<ObjectId> taskIds = taskCandidates.stream()
				.map(TaskDto::getId)
				.filter(Objects::nonNull)
				.collect(Collectors.toSet());
		if (CollectionUtils.isEmpty(taskIds)) {
			return Collections.emptyList();
		}
		Criteria criteria = new Criteria().andOperator(
				Criteria.where("_id").in(taskIds),
				Criteria.where("is_deleted").ne(true),
				Criteria.where("status").in(TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN, TaskDto.STATUS_RUNNING)
		);
		Query query = Query.query(criteria);
		query.fields().include("_id", "name", "status", "syncType");
		List<TaskDto> tasks = taskService.findAllDto(query, user);
		if (CollectionUtils.isEmpty(tasks)) {
			return Collections.emptyList();
		}
		tasks.sort(Comparator.comparingInt(this::taskStopOrder));
		return tasks;
	}

	private void pauseAffectedTasks(List<TaskDto> affectedTasks, List<TaskDto> stoppedTasks, UserDetail user) {
		for (TaskDto task : affectedTasks) {
			log.info("Stopping task '{}' (id={}, syncType={}) before connector registration",
					task.getName(), task.getId(), task.getSyncType());
			stoppedTasks.add(task);
			taskService.pause(task.getId(), user, false);
		}
	}

	private int taskStopOrder(TaskDto task) {
		if (TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(task.getSyncType())) {
			return 2;
		}
		if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(task.getSyncType())) {
			return 1;
		}
		return 0;
	}

	private List<InspectDto> findAffectedInspects(Set<String> connectionIds, UserDetail user) {
		if (CollectionUtils.isEmpty(connectionIds)) {
			return Collections.emptyList();
		}
		Criteria criteria = new Criteria().andOperator(
				Criteria.where("is_deleted").ne(true),
				new Criteria().orOperator(
						Criteria.where("tasks.source.connectionId").in(connectionIds),
						Criteria.where("tasks.target.connectionId").in(connectionIds))
		);
		Query query = Query.query(criteria);
		query.fields().include("_id", "name", "status");
		List<InspectDto> inspectTasks = inspectService.findAllDto(query, user);
		if (CollectionUtils.isEmpty(inspectTasks)) {
			return Collections.emptyList();
		}
		return inspectTasks;
	}

	private List<InspectDto> findCurrentAffectedInspects(List<InspectDto> inspectCandidates, UserDetail user) {
		Set<ObjectId> inspectIds = inspectCandidates.stream()
				.map(InspectDto::getId)
				.filter(Objects::nonNull)
				.collect(Collectors.toSet());
		if (CollectionUtils.isEmpty(inspectIds)) {
			return Collections.emptyList();
		}
		Criteria criteria = new Criteria().andOperator(
				Criteria.where("_id").in(inspectIds),
				Criteria.where("is_deleted").ne(true),
				Criteria.where("status").in(InspectStatusEnum.RUNNING.getValue(), InspectStatusEnum.SCHEDULING.getValue())
		);
		Query query = Query.query(criteria);
		query.fields().include("_id", "name", "status");
		List<InspectDto> inspectTasks = inspectService.findAllDto(query, user);
		return CollectionUtils.isEmpty(inspectTasks) ? Collections.emptyList() : inspectTasks;
	}

	private void stopAffectedInspects(List<InspectDto> inspectTasks, List<InspectDto> stoppedInspects, UserDetail user) {
		for (InspectDto inspectTask : inspectTasks) {
			Where where = new Where();
			where.put("id", inspectTask.getId().toHexString());
			InspectDto stopDto = new InspectDto();
			stopDto.setStatus(InspectStatusEnum.STOPPING.getValue());
			log.info("Stopping inspect task '{}' (id={}) before connector registration",
					inspectTask.getName(), inspectTask.getId());
			inspectService.doExecuteInspect(where, stopDto, user);
			stoppedInspects.add(inspectTask);
		}
	}

	private void waitForAffectedResourcesStopped(List<TaskDto> stoppedTasks, List<InspectDto> stoppedInspects,
			UserDetail user, RegistrationLockRenewal lockRenewal) {
		if (CollectionUtils.isEmpty(stoppedTasks) && CollectionUtils.isEmpty(stoppedInspects)) {
			return;
		}
		long deadline = System.currentTimeMillis() + taskStopTimeoutMillis;
		while (true) {
			lockRenewal.ensureHealthy();
			List<String> pendingResources = new ArrayList<>();
			for (TaskDto stoppedTask : stoppedTasks) {
				TaskDto task = taskService.findOne(Query.query(Criteria.where("_id").is(stoppedTask.getId())), user);
				if (task != null && !isTaskInactive(task.getStatus())) {
					pendingResources.add("task " + stoppedTask.getName());
				}
			}
			for (InspectDto stoppedInspect : stoppedInspects) {
				InspectDto inspect = inspectService.findById(stoppedInspect.getId());
				if (inspect != null && !isInspectStopped(inspect.getStatus())) {
					pendingResources.add("inspect task " + stoppedInspect.getName());
				}
			}
			if (pendingResources.isEmpty()) {
				return;
			}
			if (System.currentTimeMillis() >= deadline) {
				throw new BizException("Affected resources did not stop before connector registration timeout: "
						+ String.join(", ", pendingResources));
			}
			sleepBeforeNextStatusCheck("affected resources", String.join(", ", pendingResources));
		}
	}

	private boolean isTaskInactive(String status) {
		return TaskDto.STATUS_STOP.equals(status)
				|| TaskDto.STATUS_COMPLETE.equals(status)
				|| TaskDto.STATUS_ERROR.equals(status)
				|| TaskDto.STATUS_SCHEDULE_FAILED.equals(status);
	}

	private boolean isInspectStopped(String status) {
		return InspectStatusEnum.DONE.getValue().equals(status)
				|| InspectStatusEnum.PASSED.getValue().equals(status)
				|| InspectStatusEnum.FAILED.getValue().equals(status)
				|| InspectStatusEnum.ERROR.getValue().equals(status);
	}

	private boolean isInspectInProgress(String status) {
		return InspectStatusEnum.RUNNING.getValue().equals(status)
				|| InspectStatusEnum.SCHEDULING.getValue().equals(status)
				|| InspectStatusEnum.WAITING.getValue().equals(status)
				|| InspectStatusEnum.STOPPING.getValue().equals(status);
	}

	private void sleepBeforeNextStatusCheck(String taskType, String taskName) {
		try {
			Thread.sleep(taskStopPollIntervalMillis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new BizException("Interrupted while waiting for " + taskType + " to stop: " + taskName);
		}
	}

	private void restartAffectedTasks(List<TaskDto> stoppedTasks, UserDetail user, List<String> restartFailures) {
		ListIterator<TaskDto> iterator = stoppedTasks.listIterator(stoppedTasks.size());
		while (iterator.hasPrevious()) {
			TaskDto stoppedTask = iterator.previous();
			ObjectId taskId = stoppedTask.getId();
			try {
				TaskDto task = taskService.findOne(Query.query(Criteria.where("_id").is(taskId)), user);
				if (task == null) {
					continue;
				}
				if (TaskDto.STATUS_STOPPING.equals(task.getStatus())) {
					taskService.pause(taskId, user, false, true);
				} else if (isTaskActive(task.getStatus())) {
					// Starting a task may start its shared CDC/log-collector/heartbeat dependencies as
					// part of the normal orchestration. Those dependencies can therefore already be active
					// when their own entry is reached in this restore pass; starting them again produces a
					// misleading Task.StartStatusInvalid error even though the resource was restored.
					log.info("Task '{}' (id={}) is already active after connector registration; skip duplicate restart",
							task.getName(), taskId);
				} else if (shouldRestartAfterRegistration(stoppedTask.getStatus(), task.getStatus())) {
					// Restore the task exactly as a normal start would ("11"), so the shared-mining /
					// heartbeat orchestration gated by startFlag bits 0 and 1 is not skipped. Passing "00"
					// silently dropped it: logCollector() starts the task's shared-CDC miner when that miner is
					// not running, and findAffectedTasks only selects scheduling/wait_run/running tasks, so a
					// miner or heartbeat task that was already in stop is never restored separately. The
					// orchestration is idempotent (it checks the dependency's status before starting it), so it
					// is also safe when that dependency is restarted separately in this same pass.
					// Reload by id inside TaskService as well. The DTO read above is only a status snapshot;
					// using it directly can race with the asynchronous stop callback and make start validate
					// a stale task state. The id overload performs the normal full-task reload and uses the
					// same "11" orchestration flags.
					restartTaskById(taskId, user);
				} else {
					// The task did not stop the way our pause should have left it (e.g. it completed normally).
					// Do not blindly restart it and mask that; log it for the operator to investigate.
					log.warn("Not restarting task '{}' (id={}) after connector registration because its status"
									+ " changed from '{}' to '{}' instead of stopping cleanly",
							stoppedTask.getName(), taskId, stoppedTask.getStatus(), task.getStatus());
				}
			} catch (Exception e) {
				restartFailures.add("task " + taskId + " (" + stoppedTask.getName() + "): " + e.getMessage());
				log.error("Failed to restart task {} after connector registration", taskId, e);
			}
		}
	}

	private void restartTaskById(ObjectId taskId, UserDetail user) {
		for (int attempt = 1; attempt <= 3; attempt++) {
			try {
				taskService.start(taskId, user);
				return;
			} catch (BizException e) {
				if (!"Task.StartStatusInvalid".equals(e.getErrorCode())) {
					throw e;
				}
				TaskDto currentTask = taskService.findOne(Query.query(Criteria.where("_id").is(taskId)), user);
				if (currentTask != null && isTaskActive(currentTask.getStatus())) {
					log.info("Task '{}' was already restarted concurrently after connector registration", taskId);
					return;
				}
				if (currentTask != null && TaskDto.STATUS_STOPPING.equals(currentTask.getStatus())) {
					currentTask = waitForTaskReadyToRestart(taskId, user);
					if (currentTask != null && isTaskActive(currentTask.getStatus())) {
						log.info("Task '{}' was restarted concurrently while waiting for stop to complete", taskId);
						return;
					}
				}
				if (currentTask == null || !isTaskStartable(currentTask.getStatus()) || attempt == 3) {
					throw e;
				}
				log.info("Retrying task '{}' restart after transient start status race (attempt {})", taskId, attempt + 1);
			}
		}
	}

	private TaskDto waitForTaskReadyToRestart(ObjectId taskId, UserDetail user) {
		long deadline = System.currentTimeMillis() + taskStopTimeoutMillis;
		TaskDto currentTask;
		do {
			currentTask = taskService.findOne(Query.query(Criteria.where("_id").is(taskId)), user);
			if (currentTask == null || !TaskDto.STATUS_STOPPING.equals(currentTask.getStatus())) {
				return currentTask;
			}
			if (System.currentTimeMillis() >= deadline) {
				return currentTask;
			}
			sleepBeforeNextStatusCheck("task", taskId.toHexString());
		} while (true);
	}

	private boolean isTaskStartable(String status) {
		return TaskDto.STATUS_EDIT.equals(status)
				|| TaskDto.STATUS_STOP.equals(status)
				|| TaskDto.STATUS_COMPLETE.equals(status)
				|| TaskDto.STATUS_ERROR.equals(status)
				|| TaskDto.STATUS_SCHEDULE_FAILED.equals(status)
				|| TaskDto.STATUS_WAIT_START.equals(status);
	}

	private boolean isTaskActive(String status) {
		return TaskDto.STATUS_SCHEDULING.equals(status)
				|| TaskDto.STATUS_WAIT_RUN.equals(status)
				|| TaskDto.STATUS_RUNNING.equals(status);
	}

	private boolean shouldRestartAfterRegistration(String statusBeforeStop, String statusNow) {
		boolean wasActiveBeforeStop = isTaskActive(statusBeforeStop);
		if (wasActiveBeforeStop && TaskDto.STATUS_ERROR.equals(statusNow)) {
			// A running task can receive a late engine error callback while its asynchronous pause is
			// completing. It still needs the normal restart attempt; only a failed restart should be
			// reported as a registration failure.
			return true;
		}
		if (TaskDto.STATUS_STOP.equals(statusNow)) {
			// A task captured as SCHEDULING can end up in STOP rather than SCHEDULE_FAILED: the status is
			// snapshotted when the affected tasks are selected, but pause() runs later, and by then the
			// task has usually moved on to wait_run/running, so pausing it lands in STOP instead of
			// SCHEDULE_FAILED. Treat it as active so it is restored rather than silently left stopped while
			// the registration still reports success.
			return TaskDto.STATUS_RUNNING.equals(statusBeforeStop)
					|| TaskDto.STATUS_WAIT_RUN.equals(statusBeforeStop)
					|| TaskDto.STATUS_SCHEDULING.equals(statusBeforeStop);
		}
		if (TaskDto.STATUS_SCHEDULE_FAILED.equals(statusNow)) {
			// An active task may be snapshotted before pause and transition to schedule_failed
			// while stop is being requested. Restore it like any other active pre-stop task.
			return isTaskActive(statusBeforeStop);
		}
		return false;
	}

	private void restartAffectedInspects(List<InspectDto> stoppedInspects, UserDetail user, List<String> restartFailures) {
		ListIterator<InspectDto> iterator = stoppedInspects.listIterator(stoppedInspects.size());
		while (iterator.hasPrevious()) {
			InspectDto stoppedInspect = iterator.previous();
			ObjectId inspectId = stoppedInspect.getId();
			try {
				InspectDto inspect = inspectService.findById(inspectId);
				if (inspect == null) {
					String failure = "inspect task " + inspectId + " (" + stoppedInspect.getName()
							+ ") disappeared before it could be restarted";
					restartFailures.add(failure);
					log.error(failure);
					continue;
				}
				if (!isInspectStopped(inspect.getStatus())) {
					if (isInspectInProgress(inspect.getStatus())) {
						log.warn("Not restarting inspect task '{}' (id={}) after connector registration because stop is still pending (status={})",
								stoppedInspect.getName(), inspectId, inspect.getStatus());
						continue;
					}
					String failure = "inspect task " + inspectId + " (" + stoppedInspect.getName()
							+ ") has unexpected status '" + inspect.getStatus() + "' and was not restarted";
					restartFailures.add(failure);
					log.error(failure);
					continue;
				}
				Where where = new Where();
				where.put("id", inspectId.toHexString());
				InspectDto scheduleDto = new InspectDto();
				scheduleDto.setStatus(InspectStatusEnum.SCHEDULING.getValue());
				inspectService.doExecuteInspect(where, scheduleDto, user);
			} catch (Exception e) {
				restartFailures.add("inspect task " + inspectId + " (" + stoppedInspect.getName() + "): " + e.getMessage());
				log.error("Failed to restart inspect task {} after connector registration", inspectId, e);
			}
		}
	}
	public String checkJarMD5(String pdkHash, int pdkBuildNumber, String fileName){
		String md5 = null;
		Criteria criteria = Criteria.where(METADATA_PDK_HASH).is(pdkHash);
		Query query = new Query(criteria);
		criteria.and(METADATA_PDK_APIBUILD_NUMBER).lte(pdkBuildNumber);
		criteria.and("filename").is(fileName);
		query.with(Sort.by(METADATA_PDK_APIBUILD_NUMBER).descending().and(Sort.by("uploadDate").descending()));
		GridFSFile gridFSFile = fileService.findOne(query);
		if(null != gridFSFile && null != gridFSFile.getMetadata()){
			md5 = (String) gridFSFile.getMetadata().get("md5");
		}
		return md5;
	}
	public String checkJarMD5(String pdkHash, int pdkBuildNumber){
		String md5 = null;
		Criteria criteria = Criteria.where(METADATA_PDK_HASH).is(pdkHash);
		Query query = new Query(criteria);
		criteria.and(METADATA_PDK_APIBUILD_NUMBER).lte(pdkBuildNumber);
		query.with(Sort.by(METADATA_PDK_APIBUILD_NUMBER).descending());
		GridFSFile gridFSFile = fileService.findOne(query);
		if(null != gridFSFile && null != gridFSFile.getMetadata()){
			md5 = (String) gridFSFile.getMetadata().get("md5");
		}
		return md5;
	}
	public String checkJarMD5(String pdkHash, String fileName){
		String md5 = null;
		Criteria criteria = Criteria.where(METADATA_PDK_HASH).is(pdkHash).and("filename").is(fileName);
		Query query = new Query(criteria);
		GridFSFile gridFSFile = fileService.findOne(query);
		if(null != gridFSFile && null != gridFSFile.getMetadata()){
			md5 = (String) gridFSFile.getMetadata().get("md5");
		}
		return md5;
	}

	public void uploadAndView(String pdkHash, Integer pdkBuildNumber, UserDetail user, PdkFileTypeEnum type, HttpServletResponse response) {
		Criteria criteria = Criteria.where("pdkHash").is(pdkHash);
		Query query = new Query(criteria);

		switch (type) {
			case JAR:
				query.fields().include("jarRid");
				criteria.and("pdkAPIBuildNumber").lte(pdkBuildNumber);
				break;
			case IMAGE:
				query.fields().include("icon");
				break;
			case MARKDOWN:
				query.fields().include("messages");
				break;
			default:
		}
		query.with(Sort.by("pdkAPIBuildNumber").descending());

		DataSourceDefinitionDto one = dataSourceDefinitionService.findOne(query);

		if (one == null) {
			log.error("pdkHash is error pdkHash:{}", pdkHash);
			try {
				response.sendError(404);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return;
		}

		if ("customer".equals(one.getScope()) && !user.getCustomerId().equals(one.getCustomId())) {
			throw new BizException("PDK.DOWNLOAD.SOURCE.FAILED");
		}

		String resourceId;
		switch (type) {
			case JAR:
				resourceId = one.getJarRid();
				break;
			case IMAGE:
				resourceId = one.getIcon();
				break;
			case MARKDOWN:
				String language = MessageUtil.getLanguage();
				LinkedHashMap<String, Object> messages = one.getMessages();
				if (messages != null) {
					Object lan = messages.get(language);
					if (Objects.nonNull(lan) && Objects.nonNull(((Map<?, ?>) lan).get("doc"))) {
						Object docId = ((Map<?, ?>) lan).get("doc");
						resourceId = docId.toString();
					} else {
						resourceId = "";
					}
				} else {
					resourceId = "";
				}
				break;
			default:
				resourceId = "";
		}

		if (StringUtils.isBlank(resourceId)) {
//            throw new BizException("SystemError");
			try {
				response.sendError(404);
				return;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		fileService.viewImg(MongoUtils.toObjectId(resourceId), response);
	}

    protected void uploadDocs(Map<String, MultipartFile> docMap, LinkedHashMap<String, Object> messages, Map<String, Object> fileInfo, Map<String, Object> oemConfig) throws IOException {
        if (docMap == null || docMap.isEmpty() || null == messages) return;

        Map<String, ObjectId> deduplicatioMap = new HashMap<>();
        for (Object v : messages.values()) {
            if (!(v instanceof Map)) continue;

            for (Map.Entry<String, Object> langEntry : ((Map<String, Object>) v).entrySet()) {
                if (null == langEntry.getValue()
                    || !("doc".equals(langEntry.getKey()) || langEntry.getKey().startsWith("doc:"))) {
                    continue;
                }

                String path = langEntry.getValue().toString();
                if (deduplicatioMap.containsKey(path)) {
                    langEntry.setValue(deduplicatioMap.get(path));
                    continue;
                }
				MultipartFile doc = docMap.getOrDefault(path, null);

                ObjectId docId = fileService.storeFile(
                    OEMReplaceUtil.replace(doc.getInputStream(), oemConfig),
                    doc.getOriginalFilename(),
                    null,
                    fileInfo);
                langEntry.setValue(docId);
                deduplicatioMap.put(path, docId);
            }
        }
    }

    public void downloadDoc(String pdkHash, Integer pdkBuildNumber, String filename, UserDetail user, HttpServletResponse response) throws IOException {
        Criteria criteria = Criteria.where("pdkHash").is(pdkHash);
        if (null != pdkBuildNumber) {
            criteria.and("pdkAPIBuildNumber").is(pdkBuildNumber);
        }

        Query query = new Query(criteria);
        query.fields().include("messages");
        query.with(Sort.by("pdkAPIBuildNumber").descending());

        DataSourceDefinitionDto one = dataSourceDefinitionService.findOne(query);
        if (one == null) {
            log.warn("Not found any datasource: {}", query.getQueryObject().toJson());
            response.sendError(404);
            return;
        }

        if ("customer".equals(one.getScope()) && !user.getCustomerId().equals(one.getCustomId())) {
            throw new BizException("PDK.DOWNLOAD.SOURCE.FAILED");
        }

        String language = MessageUtil.getLanguage();
        String resourceId = Optional.ofNullable(one.getMessages())
            .map(messages -> messages.get(language))
            .map(langMap-> ((Map<?,?>)langMap).get(filename))
            .map(Object::toString)
            .orElse("");

        if (StringUtils.isBlank(resourceId)) {
            response.sendError(404);
            return;
        }

        fileService.viewImg(MongoUtils.toObjectId(resourceId), response);
    }

	public List<PdkVersionCheckDto> versionCheck(int days) {
		List<PdkVersionCheckDto> result = Lists.newArrayList();

		Query query = Query.query(Criteria.where("is_deleted").is(false).and("scope").is("public"));
		List<DataSourceDefinitionDto> all = dataSourceDefinitionService.findAll(query);
		if (CollectionUtils.isNotEmpty(all)) {
//             get tcm build info
			String tcmReleaseTemp = tcmService.getLatestProductReleaseCreateTime();
//            String tcmReleaseTemp = "2023-04-28T03:27:04.856+00:00";
			if (StringUtils.isBlank(tcmReleaseTemp)) {
				tcmReleaseTemp = DateUtil.now();
			}

			List<DataSourceDefinitionDto> list = all.stream()
					.collect(Collectors.groupingBy(DataSourceDefinitionDto::getPdkId))
					.values().stream()
					.map(group -> group.stream()
							.max(Comparator.comparing(DataSourceDefinitionDto::getPdkAPIBuildNumber))
							.orElse(null))
					.collect(Collectors.toList());

			String finalTcmReleaseTemp = tcmReleaseTemp;
			list.forEach(info -> {
				PdkVersionCheckDto checkDto = PdkVersionCheckDto.builder().pdkId(info.getPdkId()).pdkVersion(info.getPdkAPIVersion()).pdkHash(info.getPdkHash()).isLatest(info.isLatest()).build();

				Date buildDate;
				Map<String, String> manifest = info.getManifest();
				if (Objects.nonNull(manifest) && Objects.nonNull(manifest.get("Git-Build-Time"))) {
					String buildTime = manifest.get("Git-Build-Time");
					buildDate = DateUtil.parse(buildTime, "yyyy-MM-dd'T'HH:mm:ssZ");

					checkDto.setGitBuildUserName(manifest.get("Git-Build-User-Name"));
					checkDto.setGitBranch(manifest.get("Git-Branch"));
					checkDto.setGitCommitId(manifest.get("Git-Commit-Id"));

				} else {
					buildDate = info.getLastUpdAt();
				}
				checkDto.setGitBuildTime(DateUtil.formatDateTime(buildDate));

				Date tcmReleaseDate = DateUtil.parseDate(finalTcmReleaseTemp);
				boolean isLatest = tcmReleaseDate.before(buildDate) || ChronoUnit.DAYS.between(buildDate.toInstant(), tcmReleaseDate.toInstant()) <= days;
				// compare with tcm build info
				checkDto.setLatest(isLatest);
				result.add(checkDto);
			});
		}

		// sort by isLatest asc
		result.sort(Comparator.comparing(PdkVersionCheckDto::isLatest));
		return result;
	}
}
