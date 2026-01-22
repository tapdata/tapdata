package com.tapdata.tm.group.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import cn.hutool.core.date.DateUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.group.dto.*;
import com.tapdata.tm.group.entity.GroupInfoEntity;
import com.tapdata.tm.group.handler.ResourceHandler;
import com.tapdata.tm.group.handler.ResourceHandlerRegistry;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import com.tapdata.tm.group.service.git.GitBaseService;
import com.tapdata.tm.group.service.git.GitService;
import com.tapdata.tm.group.service.git.GitServiceRouter;
import com.tapdata.tm.group.service.transfer.GroupExportRequest;
import com.tapdata.tm.group.service.transfer.GroupImportRequest;
import com.tapdata.tm.group.service.transfer.GroupTransferStrategy;
import com.tapdata.tm.group.service.transfer.GroupTransferStrategyRegistry;
import com.tapdata.tm.group.service.transfer.GroupTransferType;
import com.tapdata.tm.group.vo.ExportGroupRequest;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import jakarta.servlet.http.HttpServletResponse;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.data.mongodb.core.query.Update;

@Service
@Slf4j
@Setter(onMethod_ = { @Autowired })
public class GroupInfoService extends BaseService<GroupInfoDto, GroupInfoEntity, ObjectId, GroupInfoRepository> {

    public GroupInfoService(@NotNull GroupInfoRepository repository) {
        super(repository, GroupInfoDto.class, GroupInfoEntity.class);
    }

    private TaskService taskService;
    private ModulesService modulesService;
    private DataSourceService dataSourceService;
    private MetadataInstancesService metadataInstancesService;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private GroupInfoRecordService groupInfoRecordService;
    private ResourceHandlerRegistry resourceHandlerRegistry;
    private InspectService inspectService;
    private GroupTransferStrategyRegistry transferStrategyRegistry;
	private GitServiceRouter gitServiceRouter;

    @Override
    protected void beforeSave(GroupInfoDto dto, UserDetail userDetail) {
		checkGitInfoAndHandleUrl(dto);
	}

	private static void checkGitInfoAndHandleUrl(GroupInfoDto dto) {
		GroupGitInfoDto gitInfo = dto.getGitInfo();
		if (null != gitInfo) {
			String repoUrl = gitInfo.getRepoUrl();
			// check repo url format
			// 1. not blank
			// 2. start with https://
			if (StringUtils.isBlank(repoUrl) || !repoUrl.startsWith("https://")) {
				throw new BizException("Group.GitInfo.RepoUrl.FormatError");
			}
			// Add .git as prefix if not exists
			if (!repoUrl.endsWith(".git")) {
				gitInfo.setRepoUrl(repoUrl + ".git");
			}
		}
	}

	@Override
	public UpdateResult update(Query query, GroupInfoDto dto) {
		checkGitInfoAndHandleUrl(dto);
		return super.update(query, dto);
	}

	public Page<GroupInfoDto> groupList(Filter filter, UserDetail userDetail) {
        Map<String, Object> where = filter.getWhere();
        Map<String, Object> notDeleteMap = new HashMap<>();
        notDeleteMap.put("$ne", true);
        where.put("is_deleted", notDeleteMap);
        Page<GroupInfoDto> groupInfoDtoPage = super.find(filter, userDetail);

        Map<ResourceType, Set<String>> resourceIdsByType = extractResourceIdsByType(groupInfoDtoPage.getItems());

        // 通过资源处理器加载资源名称
        Map<ResourceType, Map<String, String>> nameMapsByType = new HashMap<>();
        for (Map.Entry<ResourceType, Set<String>> entry : resourceIdsByType.entrySet()) {
           ResourceHandler handler = resourceHandlerRegistry.getHandler(entry.getKey());
            if (handler != null && CollectionUtils.isNotEmpty(entry.getValue())) {
                List<?> resources = handler.loadResources(new ArrayList<>(entry.getValue()), userDetail);
                Map<String, String> nameMap = new HashMap<>();
                for (Object resource : resources) {
                    String id = getResourceId(resource);
                    String name = getResourceName(resource);
                    if (id != null && name != null) {
                        nameMap.put(id, name);
                    }
                }
                nameMapsByType.put(entry.getKey(), nameMap);
            }
        }

        // 为资源项设置名称
        for (GroupInfoDto groupInfoDto : groupInfoDtoPage.getItems()) {
            if (CollectionUtils.isNotEmpty(groupInfoDto.getResourceItemList())) {
                for (ResourceItem item : groupInfoDto.getResourceItemList()) {
                    if (item != null && item.getType() != null) {
                        Map<String, String> nameMap = nameMapsByType.get(item.getType());
                        if (nameMap != null && item.getId() != null) {
                            item.setName(nameMap.get(item.getId()));
                        }
                    }
                }
            }
        }
        return groupInfoDtoPage;
    }

    public void removeResourceReferences(List<String> resourceIds, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return;
        }
        List<ObjectId> cleanedIds = resourceIds.stream()
                .filter(StringUtils::isNotBlank)
                .distinct()
                .map(MongoUtils::toObjectId)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(cleanedIds)) {
            return;
        }
        try {
            Query query = new Query();
            Criteria resourceCriteria = Criteria.where("resourceItemList._id").in(cleanedIds);
            query.addCriteria(resourceCriteria);;
            Update update = new Update()
                    .pull("resourceItemList", new Document("_id", new Document("$in", cleanedIds)));
            update(query, update, userDetail);
        } catch (Exception e) {
            log.warn("Failed to remove group resource references, ids={}", cleanedIds, e);
        }
    }

    public void exportGroupInfos(HttpServletResponse response, ExportGroupRequest exportGroupRequest, UserDetail user) {
		List<String> groupIds = exportGroupRequest.getGroupIds();
		Map<String, List<String>> groupResetTask = exportGroupRequest.getGroupResetTask();

        List<GroupInfoDto> groupInfos = loadGroupInfosByIds(groupIds, user);

        if (CollectionUtils.isEmpty(groupInfos)) {
            throw new BizException("GroupInfo.Not.Found");
        }

		// Check if any group is currently being exported via Git
		GroupTransferType groupTransferType = exportGroupRequest.getGroupTransferType();
		if (groupTransferType == GroupTransferType.GIT) {
			checkExportingGroups(groupIds, user);
		}

        applyResetTaskList(groupInfos, groupResetTask);

        // 按资源类型提取资源 ID
        Map<ResourceType, Set<String>> resourceIdsByType = extractResourceIdsByType(groupInfos);

        // 通过资源处理器加载各类资源
        Map<ResourceType, List<?>> resourcesByType = new LinkedHashMap<>();
        for (Map.Entry<ResourceType, Set<String>> entry : resourceIdsByType.entrySet()) {
            ResourceHandler handler = resourceHandlerRegistry.getHandler(entry.getKey());
            if (handler != null) {
                List<?> resources = handler.loadResources(new ArrayList<>(entry.getValue()), user);
                resourcesByType.put(entry.getKey(), resources);
            }
        }

        // 构建导出 payload
        Map<String, List<TaskUpAndLoadDto>> payloadsByType = new LinkedHashMap<>();
        for (Map.Entry<ResourceType, List<?>> entry : resourcesByType.entrySet()) {
            ResourceHandler handler = resourceHandlerRegistry.getHandler(entry.getKey());
            if (handler != null) {
                handler.handleRelatedResources(payloadsByType, entry.getValue(), user);
            }
        }
        for (Map.Entry<ResourceType, List<?>> entry : resourcesByType.entrySet()) {
            ResourceHandler handler = resourceHandlerRegistry.getHandler(entry.getKey());
            if (handler != null) {
                List<TaskUpAndLoadDto> payload = handler.buildExportPayload(entry.getValue(), user);
                payloadsByType.put(entry.getKey().name(), payload);
            }
        }
        List<TaskUpAndLoadDto> groupInfoPayload = buildGroupInfoPayload(groupInfos);

        // 构建导出文件内容
        Map<String, byte[]> contents = new LinkedHashMap<>();
        contents.put("GroupInfo.json", Objects.requireNonNull(JsonUtil.toJsonUseJackson(groupInfoPayload))
                .getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, List<TaskUpAndLoadDto>> entry : payloadsByType.entrySet()) {
            contents.put(ResourceType.getResourceName(entry.getKey()),
                    Objects.requireNonNull(JsonUtil.toJsonUseJackson(entry.getValue()))
                            .getBytes(StandardCharsets.UTF_8));
        }

        String yyyymmdd = DateUtil.today().replaceAll("-", "");
        String name = buildGroupExportFileName(groupInfos, yyyymmdd);

        log.info("Start exporting groups, groupCount={}, user={}", groupInfos.size(), user.getUsername());

        // 构建导出记录
		GroupInfoRecordDto recordDto = buildExportRecord(GroupInfoRecordDto.TYPE_EXPORT, user,
				buildExportRecordDetails(groupInfos, resourcesByType), name, exportGroupRequest, groupInfos.get(0));
        recordDto = groupInfoRecordService.save(recordDto, user);

		doExport(response, exportGroupRequest, user, contents, name, groupInfos, recordDto);
	}

	private void doExport(HttpServletResponse response, ExportGroupRequest exportGroupRequest, UserDetail user, Map<String, byte[]> contents, String name, List<GroupInfoDto> groupInfos, GroupInfoRecordDto recordDto) {
		GroupTransferType groupTransferType = exportGroupRequest.getGroupTransferType();
		if (groupTransferType.isAsync()) {
			// Execute asynchronously
			GroupInfoService groupInfoService = SpringContextHelper.getBean(GroupInfoService.class);
			groupInfoService.executeExportAsync(response, exportGroupRequest, user, contents, name, groupInfos, recordDto);
		} else {
			// Execute synchronously
			performExport(response, exportGroupRequest, user, contents, name, groupInfos, recordDto, false);
		}
	}

	/**
	 * Execute the actual export process asynchronously.
	 */
	@Async
	public void executeExportAsync(HttpServletResponse response, ExportGroupRequest exportGroupRequest, UserDetail user, Map<String, byte[]> contents, String name, List<GroupInfoDto> groupInfos, GroupInfoRecordDto recordDto) {
		log.info("Async export started, recordId={}, fileName={}", recordDto.getId(), name);
		performExport(response, exportGroupRequest, user, contents, name, groupInfos, recordDto, true);
	}

	/**
	 * Perform the actual export operation.
	 *
	 * @param isAsync Whether this is an async execution (affects logging)
	 */
	private void performExport(HttpServletResponse response, ExportGroupRequest exportGroupRequest, UserDetail user, Map<String, byte[]> contents, String name, List<GroupInfoDto> groupInfos, GroupInfoRecordDto recordDto, boolean isAsync) {
		GroupTransferType groupTransferType = exportGroupRequest.getGroupTransferType();
		try {
			GroupTransferStrategy strategy = transferStrategyRegistry.getStrategy(groupTransferType);
			if (strategy == null) {
				throw new BizException("GroupInfo.TransferStrategy.NotFound");
			}
			strategy.exportGroups(new GroupExportRequest(response, contents, name, groupInfos.get(0), exportGroupRequest.getGitTag(), recordDto));
			updateRecordStatus(recordDto.getId(), GroupInfoRecordDto.STATUS_COMPLETED, null,
					recordDto.getDetails(), user);
			if (groupTransferType.equals(GroupTransferType.GIT)) {
				updateGitOperationStep(recordDto, user);
			}
			if (isAsync) {
				log.info("Async export completed successfully, recordId={}, fileName={}, groupCount={}", recordDto.getId(), name, groupInfos.size());
			} else {
				log.info("Group export completed successfully, groupCount={}, fileName={}", groupInfos.size(), name);
			}
		} catch (Exception e) {
			updateRecordStatus(recordDto.getId(), GroupInfoRecordDto.STATUS_FAILED, e.getMessage(),
					recordDto.getDetails(), user);
			if (groupTransferType.equals(GroupTransferType.GIT)) {
				updateGitOperationStep(recordDto, user);
			}
			if (isAsync) {
				log.error("Async export failed, recordId={}, fileName={}, groupCount={}, error={}", recordDto.getId(), name, groupInfos.size(),
						ThrowableUtils.getStackTraceByPn(e));
			} else {
				log.error("Group export failed, groupCount={}, error={}", groupInfos.size(),
						ThrowableUtils.getStackTraceByPn(e));
			}
		}
	}

	/**
     * Batch import groups asynchronously.
     * Returns the record ID immediately, and the import is executed in the
     * background.
     *
     * @param resource   The tar file to import
     * @param user       User details
     * @param importMode Import mode
     * @return Record ID for tracking import progress
     */
    public ObjectId batchImportGroup(Object resource, UserDetail user, ImportModeEnum importMode)
            throws IOException {
        GroupTransferStrategy strategy = transferStrategyRegistry.getStrategy(GroupTransferType.FILE);;
        if (strategy == null) {
            throw new BizException("GroupInfo.TransferStrategy.NotFound");
        }
        return strategy.importGroups(new GroupImportRequest(resource, user, importMode));
    }

    public ObjectId batchImportGroupInternal(Map<String, List<TaskUpAndLoadDto>> payloads, UserDetail user, ImportModeEnum importMode,String name){
        if (importMode == null) {
            importMode = ImportModeEnum.GROUP_IMPORT;
        }
        // Create import record with initial progress
        GroupInfoRecordDto recordDto = buildRecord(GroupInfoRecordDto.TYPE_IMPORT, user, new ArrayList<>(), name);
        recordDto.setProgress(0);
        recordDto = groupInfoRecordService.save(recordDto, user);

        ObjectId recordId = recordDto.getId();

        // Execute import asynchronously
        GroupInfoService groupInfoService = SpringContextHelper.getBean(GroupInfoService.class);
        groupInfoService.executeImportAsync(payloads, user, importMode, name, recordId);
        return recordId;
    }

    /**
     * Execute the actual import process asynchronously.
     */
    @Async
    public void executeImportAsync(Map<String, List<TaskUpAndLoadDto>> payloads, UserDetail user,
            ImportModeEnum importMode, String fileName, ObjectId recordId) {
        log.info("Async import started, recordId={}, fileName={}", recordId, fileName);

        List<TaskUpAndLoadDto> groupPayload = payloads.getOrDefault("GroupInfo.json", Collections.emptyList());

        // 使用资源处理器收集各种资源
        Map<ResourceType, Map<String, ?>> resourceMapsByType = new LinkedHashMap<>();
        Map<ResourceType, List<MetadataInstancesDto>> metadataByType = new LinkedHashMap<>();

        for (ResourceType type : ResourceType.values()) {
            ResourceHandler handler = resourceHandlerRegistry.getHandler(type);
            if (handler != null) {
                String filename = ResourceType.getResourceName(type.name());
                List<TaskUpAndLoadDto> payload = payloads.getOrDefault(filename, Collections.emptyList());
                Map<String, Object> resourceMap = new LinkedHashMap<>();
                List<MetadataInstancesDto> metadata = new ArrayList<>();
                handler.collectPayload(payload, resourceMap, metadata);
                resourceMapsByType.put(type, resourceMap);
                metadataByType.put(type, metadata);
                handler.collectPayloadRelatedResources(payloads, resourceMapsByType, metadataByType);
            }
        }

        List<GroupInfoDto> groupInfos = new ArrayList<>();
        collectGroupInfoPayload(groupPayload, groupInfos);

        List<GroupInfoRecordDetail> details = new ArrayList<>();
        try {
            details = buildImportRecordDetails(groupInfos, resourceMapsByType);
            // Calculate total resource count for progress tracking
            int connectionCount = ((Map<?, ?>) resourceMapsByType.getOrDefault(ResourceType.CONNECTION,
                    Collections.emptyMap())).size();
            int taskCount = ((Map<?, ?>) resourceMapsByType.getOrDefault(ResourceType.MIGRATE_TASK,
                    Collections.emptyMap())).size()
                    + ((Map<?, ?>) resourceMapsByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyMap()))
                            .size()
                    + ((Map<?, ?>) resourceMapsByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyMap()))
                            .size();
            int inspectCount = ((Map<?, ?>) resourceMapsByType.getOrDefault(ResourceType.INSPECT_TASK,
                    Collections.emptyMap())).size();
            int moduleCount = ((Map<?, ?>) resourceMapsByType.getOrDefault(ResourceType.MODULE, Collections.emptyMap()))
                    .size();
            int groupCount = groupInfos.size();
            int totalResources = connectionCount + taskCount + inspectCount + moduleCount + groupCount;
            int importedResources = 0;

            // Update progress after initialization
            updateImportProgress(recordId, calculateProgress(importedResources, totalResources), details, user);

            // Stage 1: Import connections
            Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
            Map<String, String> taskIdMap = new HashMap<>();
            Map<String, String> nodeIdMap = new HashMap<>();

            log.info("Stage 1: Start importing connection resources");
            Map<String, DataSourceConnectionDto> connections = (Map<String, DataSourceConnectionDto>) resourceMapsByType
                    .getOrDefault(ResourceType.CONNECTION, Collections.emptyMap());
            List<MetadataInstancesDto> connectionMetadata = metadataByType
                    .getOrDefault(ResourceType.CONNECTION, Collections.emptyList());
            conMap = dataSourceService.batchImport(new ArrayList<>(connections.values()), user, importMode);
            metadataInstancesService.batchImport(connectionMetadata, user, conMap, new HashMap<>(),
                    new HashMap<>());
            log.info("Connection resources import completed, connectionCount={}, metadataCount={}",
                    connections.size(), connectionMetadata.size());
            importedResources += connectionCount;
            updateImportProgress(recordId, calculateProgress(importedResources, totalResources), details, user);

            // Stage 2: Import tasks (MIGRATE_TASK, SYNC_TASK, SHARE_CACHE)
            log.info("Stage 2: Start importing task resources");
            List<TaskDto> allTasks = new ArrayList<>();
            Map<String, TaskDto> migrateTasks = (Map<String, TaskDto>) resourceMapsByType
                    .getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyMap());
            Map<String, TaskDto> syncTasks = (Map<String, TaskDto>) resourceMapsByType
                    .getOrDefault(ResourceType.SYNC_TASK, Collections.emptyMap());
            Map<String, TaskDto> shareCacheTasks = (Map<String, TaskDto>) resourceMapsByType
                    .getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyMap());
            allTasks.addAll(migrateTasks.values());
            allTasks.addAll(syncTasks.values());
            allTasks.addAll(shareCacheTasks.values());
            Map<String,Object> taskImportResult;
            Map<String,Object> moduleImportResult;
            if (CollectionUtils.isNotEmpty(allTasks)) {
                List<String> resetTaskList = collectResetTaskList(groupInfos);
                taskImportResult = taskService.batchImport(allTasks, user, importMode, new ArrayList<>(), conMap, taskIdMap,
                        nodeIdMap, resetTaskList);
                List<MetadataInstancesDto> allTaskMetadata = new ArrayList<>();
                allTaskMetadata
                        .addAll(metadataByType.getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyList()));
                allTaskMetadata
                        .addAll(metadataByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyList()));
                allTaskMetadata
                        .addAll(metadataByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyList()));
                metadataInstancesService.batchImport(allTaskMetadata, user, conMap, taskIdMap, nodeIdMap);
                log.info("Task resources import completed, taskCount={}, metadataCount={}",
                        allTasks.size(), allTaskMetadata.size());
            } else {
                taskImportResult = null;
            }
            importedResources += taskCount;
            updateImportProgress(recordId, calculateProgress(importedResources, totalResources), details, user);

            // Stage 3: Import inspect tasks
            Map<String, InspectDto> inspectTasks = (Map<String, InspectDto>) resourceMapsByType
                    .getOrDefault(ResourceType.INSPECT_TASK, Collections.emptyMap());
            if (MapUtils.isNotEmpty(inspectTasks)) {
                inspectService.importTaskByGroup(new ArrayList<>(inspectTasks.values()), taskIdMap, conMap, user);
                log.info("Inspect tasks import completed, inspectTaskCount={}", inspectTasks.size());
            }
            importedResources += inspectCount;
            updateImportProgress(recordId, calculateProgress(importedResources, totalResources), details, user);

            // Stage 4: Import modules
            Map<String, ModulesDto> modules = (Map<String, ModulesDto>) resourceMapsByType
                    .getOrDefault(ResourceType.MODULE, Collections.emptyMap());
            if (MapUtils.isNotEmpty(modules)) {
                moduleImportResult = modulesService.batchImport(new ArrayList<>(modules.values()), user, importMode, conMap, null);
                metadataInstancesService.batchImport(
                        metadataByType.getOrDefault(ResourceType.MODULE, Collections.emptyList()),
                        user, conMap, null, null);
            } else {
                moduleImportResult = null;
            }
            importedResources += moduleCount;
            updateImportProgress(recordId, calculateProgress(importedResources, totalResources), details, user);

            // Stage 5: Save group information
            log.info("Stage 5: Start saving group information");
            if (CollectionUtils.isNotEmpty(groupInfos)) {
                List<GroupInfoRecordDetail> finalDetails = details;
                groupInfos.forEach(groupInfo -> {
                    GroupInfoRecordDetail groupInfoRecordDetail = finalDetails.stream()
                            .filter(d -> d.getGroupId().equals(groupInfo.getId().toHexString()))
                            .findFirst().orElse(null);
                    groupInfo.setCreateUser(null);
                    groupInfo.setCustomId(null);
                    groupInfo.setLastUpdBy(null);
                    groupInfo.setUserId(null);
                    groupInfo.setResourceItemList(mapResourceItems(groupInfo.getResourceItemList(), taskIdMap,groupInfoRecordDetail,moduleImportResult,taskImportResult));
                    groupInfo.setId(null);
                    upsertByWhere(Where.where("name", groupInfo.getName()), groupInfo, user);
                });
            }
            importedResources += groupCount;
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, details, user);
            log.info("Async import completed successfully, recordId={}, fileName={}, groupCount={}",
                    recordId, fileName, groupInfos.size());
        } catch (Exception e) {
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getStackTrace(e), details, user);
            log.error("Async import failed, recordId={}, fileName={}, error={}",
                    recordId, fileName, ThrowableUtils.getStackTraceByPn(e));
        }
    }

    protected List<GroupInfoDto> loadGroupInfosByIds(List<String> groupIds, UserDetail user) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return new ArrayList<>();
        }
        List<ObjectId> ids = groupIds.stream().filter(Objects::nonNull).map(ObjectId::new).collect(Collectors.toList());
        Query query = new Query(Criteria.where("_id").in(ids).and("is_deleted").ne(true));
        return findAllDto(query, user);
    }

	/**
	 * Check if any group is currently being exported via Git
	 * @param groupIds List of group IDs to check
	 * @param user Current user
	 * @throws BizException if any group is currently being exported
	 */
	protected void checkExportingGroups(List<String> groupIds, UserDetail user) {
		if (CollectionUtils.isEmpty(groupIds)) {
			return;
		}

		// Query for records with status=exporting, type=export, groupTransferType=GIT
		// and details.groupId in groupIds
		Query query = new Query();
		query.addCriteria(Criteria.where("type").is(GroupInfoRecordDto.TYPE_EXPORT)
				.and("status").is(GroupInfoRecordDto.STATUS_EXPORTING)
				.and("groupTransferType").is(GroupTransferType.GIT.name())
				.and("details.groupId").in(groupIds));

		List<GroupInfoRecordDto> exportingRecords = groupInfoRecordService.findAllDto(query, user);

		if (CollectionUtils.isNotEmpty(exportingRecords)) {
			// Extract group IDs that are currently being exported
			Set<String> exportingGroupNames = exportingRecords.stream()
					.flatMap(record -> record.getDetails().stream())
					.map(GroupInfoRecordDetail::getGroupName)
					.collect(Collectors.toSet());

			if (!exportingGroupNames.isEmpty()) {
				throw new BizException("GroupInfo.Export.InProgress", String.join(", ", exportingGroupNames));
			}
		}
	}

    protected void applyResetTaskList(List<GroupInfoDto> groupInfos, Map<String, List<String>> groupResetTask) {
        if (CollectionUtils.isEmpty(groupInfos) || MapUtils.isEmpty(groupResetTask)) {
            return;
        }
        for (GroupInfoDto groupInfo : groupInfos) {
            if (groupInfo == null || groupInfo.getId() == null) {
                continue;
            }
            List<String> resetTaskList = groupResetTask.get(groupInfo.getId().toHexString());
            if (CollectionUtils.isNotEmpty(resetTaskList)) {
                groupInfo.setResetTaskList(resetTaskList);
            }
        }
    }

    protected List<String> collectResetTaskList(List<GroupInfoDto> groupInfos) {
        if (CollectionUtils.isEmpty(groupInfos)) {
            return new ArrayList<>();
        }
        Set<String> resetTaskIds = new LinkedHashSet<>();
        for (GroupInfoDto groupInfo : groupInfos) {
            if (groupInfo == null || CollectionUtils.isEmpty(groupInfo.getResetTaskList())) {
                continue;
            }
            resetTaskIds.addAll(groupInfo.getResetTaskList());
        }
        return new ArrayList<>(resetTaskIds);
    }

    protected List<TaskUpAndLoadDto> buildGroupInfoPayload(List<GroupInfoDto> groupInfos) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        if (CollectionUtils.isEmpty(groupInfos)) {
            return payload;
        }
        for (GroupInfoDto groupInfo : groupInfos) {
            // Create a deep copy to avoid modifying the original object
            // This is important because the original object is still needed for Git operations
            GroupInfoDto groupInfoCopy = BeanUtil.deepClone(groupInfo, GroupInfoDto.class);

            if (groupInfoCopy != null) {
                groupInfoCopy.setCreateUser(null);
                groupInfoCopy.setCustomId(null);
                groupInfoCopy.setLastUpdBy(null);
                groupInfoCopy.setUserId(null);

                // Remove sensitive token information from gitInfo before export
                // to prevent GitHub Push Protection from blocking the push
                if (groupInfoCopy.getGitInfo() != null) {
                    groupInfoCopy.getGitInfo().setToken(null);
                }

                payload.add(new TaskUpAndLoadDto("GroupInfo", JsonUtil.toJsonUseJackson(groupInfoCopy)));
            }
        }
        return payload;
    }

    protected void collectGroupInfoPayload(List<TaskUpAndLoadDto> payload, List<GroupInfoDto> groupInfos) {
        if (CollectionUtils.isEmpty(payload)) {
            return;
        }
        for (TaskUpAndLoadDto taskUpAndLoadDto : payload) {
            if (!GroupConstants.COLLECTION_GROUP_INFO.equals(taskUpAndLoadDto.getCollectionName())
                    || StringUtils.isBlank(taskUpAndLoadDto.getJson())) {
                continue;
            }
            GroupInfoDto groupInfoDto = JsonUtil.parseJsonUseJackson(taskUpAndLoadDto.getJson(), GroupInfoDto.class);
            if (groupInfoDto != null) {
                groupInfos.add(groupInfoDto);
            }
        }
    }

	protected GroupInfoRecordDto buildExportRecord(String type, UserDetail user, List<GroupInfoRecordDetail> details,
											 String fileName, ExportGroupRequest exportGroupRequest, GroupInfoDto groupInfoDto) {
		GroupTransferType groupTransferType = exportGroupRequest.getGroupTransferType();
		GroupInfoRecordDto recordDto = new GroupInfoRecordDto();
		recordDto.setType(type);
		recordDto.setGroupTransferType(groupTransferType.name());
		switch (groupTransferType) {
			case FILE -> recordDto.setFileName(fileName);
			case GIT -> recordDto.setFileName(groupInfoDto.getGitInfo().getRepoUrl());
		}
		recordDto.setStatus(GroupInfoRecordDto.STATUS_EXPORTING);
		recordDto.setOperator(user == null ? null : user.getUsername());
		recordDto.setOperationTime(new Date());
		recordDto.setDetails(details);
		return recordDto;
	}

    protected GroupInfoRecordDto buildRecord(String type, UserDetail user, List<GroupInfoRecordDetail> details,
            String fileName) {
        GroupInfoRecordDto recordDto = new GroupInfoRecordDto();
        recordDto.setType(type);
        recordDto.setFileName(fileName);
        if (GroupInfoRecordDto.TYPE_EXPORT.equals(type)) {
            recordDto.setStatus(GroupInfoRecordDto.STATUS_EXPORTING);
        } else if (GroupInfoRecordDto.TYPE_IMPORT.equals(type)) {
            recordDto.setStatus(GroupInfoRecordDto.STATUS_IMPORTING);
        }
        recordDto.setOperator(user == null ? null : user.getUsername());
        recordDto.setOperationTime(new Date());
        recordDto.setDetails(details);
        return recordDto;
    }

    protected void updateRecordStatus(ObjectId recordId, String status, String message,
            List<GroupInfoRecordDetail> details, UserDetail user) {
        if (recordId == null) {
            return;
        }
        Update update = new Update().set("status", status);
        if (message != null) {
            update.set("message", message);
        }
        if (details != null) {
            update.set("details", details);
        }
        update.set("progress", 100);
        groupInfoRecordService.updateById(recordId, update, user);
    }

	protected void updateGitOperationStep(GroupInfoRecordDto recordDto, UserDetail user) {
		if (recordDto == null) {
			return;
		}
		ObjectId id = recordDto.getId();
		if (null == id) {
			return;
		}
		List<GitOperationStep> gitOperationSteps = recordDto.getGitOperationSteps();
		if (CollectionUtils.isEmpty(gitOperationSteps)) {
			return;
		}
		Update update = new Update().set("gitOperationSteps", gitOperationSteps);
		groupInfoRecordService.updateById(id, update, user);
	}

    /**
     * Update import progress in the record.
     *
     * @param recordId Record ID
     * @param progress Progress percentage (0-100)
     * @param details  Record details
     * @param user     User details
     */
    protected void updateImportProgress(ObjectId recordId, int progress, List<GroupInfoRecordDetail> details, UserDetail user) {
        if (recordId == null) {
            return;
        }
        Update update = new Update().set("progress", progress);
        if (details != null) {
            update.set("details", details);
        }
        groupInfoRecordService.updateById(recordId, update, user);
        log.debug("Import progress updated, recordId={}, progress={}%", recordId, progress);
    }

    /**
     * Calculate progress percentage based on imported and total resources.
     *
     * @param imported Imported resource count
     * @param total    Total resource count
     * @return Progress percentage (0-100)
     */
    protected int calculateProgress(int imported, int total) {
        if (total == 0) {
            return 0;
        }
        return Math.min(99, (int) ((imported * 100.0) / total));
    }

    /**
     * 构建导出记录详情
     */
    protected List<GroupInfoRecordDetail> buildExportRecordDetails(List<GroupInfoDto> groupInfos, Map<ResourceType, List<?>> resourcesByType) {
        List<GroupInfoRecordDetail> details = new ArrayList<>();
        for (GroupInfoDto groupInfo : groupInfos) {
            GroupInfoRecordDetail detail = new GroupInfoRecordDetail();
            if (groupInfo.getId() != null) {
                detail.setGroupId(groupInfo.getId().toHexString());
            }
            detail.setGroupName(groupInfo.getName());
            Map<ResourceType, Map<String, ?>> resourceMapsByType = new HashMap<>();
            for (Map.Entry<ResourceType, List<?>> entry : resourcesByType.entrySet()) {
                Map<String, Object> resourceMap = new HashMap<>();
                for (Object resource : entry.getValue()) {
                    String id = getResourceId(resource);
                    if (id != null) {
                        resourceMap.put(id, resource);
                    }
                }
                resourceMapsByType.put(entry.getKey(), resourceMap);
            }
            fillRecordDetails(detail, groupInfo, resourceMapsByType, GroupInfoRecordDetail.RecordAction.EXPORTED);
            details.add(detail);
        }
        return details;
    }

    protected List<ResourceItem> mapResourceItems(List<ResourceItem> items, Map<String, String> idMap,GroupInfoRecordDetail groupInfoRecordDetail,Map<String, Object> moduleImportResult,Map<String, Object> taskImportResult) {
        if (CollectionUtils.isEmpty(items)) {
            return new ArrayList<>();
        }
        List<ResourceItem> mapped = new ArrayList<>();
        Map<String, GroupInfoRecordDetail.RecordDetail> recordDetailMap = groupInfoRecordDetail.getRecordDetails().stream()
                .collect(Collectors.toMap(GroupInfoRecordDetail.RecordDetail::getResourceId, Function.identity(),
                        (oldVal, newVal) -> newVal));
        for (ResourceItem item : items) {
            if (item == null || StringUtils.isBlank(item.getId()) || item.getType() == null) {
                continue;
            }
            ResourceItem copy = new ResourceItem();
            copy.setType(item.getType());
            if (item.getType() == ResourceType.MIGRATE_TASK || item.getType() == ResourceType.SYNC_TASK) {
                String newId = idMap.get(item.getId());
                if (StringUtils.isNotBlank(newId)) {
                    copy.setId(newId);
                } else {
                    copy.setId(item.getId());
                }
            } else {
                copy.setId(item.getId());

            }
            updateRecordAction(item.getId(), copy.getId(), item.getType() == ResourceType.MODULE ? moduleImportResult : taskImportResult, recordDetailMap);
            mapped.add(copy);
        }
        return mapped;
    }

    protected void updateRecordAction(String resourceId,String newId,Map<String, Object> importResult,Map<String, GroupInfoRecordDetail.RecordDetail> recordDetailMap){
        if(MapUtils.isNotEmpty(importResult) && importResult.containsKey(newId)) {
            if(importResult.get(newId) instanceof Long count){
                if(count > 0) {
                    recordDetailMap.computeIfPresent(resourceId, (k, v) -> {
                        v.setAction(GroupInfoRecordDetail.RecordAction.REPLACED);
                        return v;
                    });
                }else {
                    recordDetailMap.computeIfPresent(resourceId, (k, v) -> {
                        v.setAction(GroupInfoRecordDetail.RecordAction.NO_UPDATE);
                        return v;
                    });
                }
            }else if(importResult.get(newId) instanceof String message){
                recordDetailMap.computeIfPresent(resourceId, (k, v) -> {
                    v.setAction(GroupInfoRecordDetail.RecordAction.ERRORED);
                    v.setMessage(message);
                    return v;
                });
            }
        }else{
            recordDetailMap.computeIfPresent(resourceId, (k, v) -> {
                v.setAction(GroupInfoRecordDetail.RecordAction.IMPORTED);
                return v;
            });
        }
    }

    protected Map<String, String> renameDuplicateGroups(List<GroupInfoDto> groupInfos, UserDetail user) {
        Map<String, String> renamedGroups = new HashMap<>();
        if (CollectionUtils.isEmpty(groupInfos)) {
            return renamedGroups;
        }

        List<String> allNames = groupInfos.stream()
                .filter(g -> g != null && StringUtils.isNotBlank(g.getName()))
                .map(GroupInfoDto::getName)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(allNames)) {
            return renamedGroups;
        }

        Query query = new Query(Criteria.where("name").in(allNames).and("is_deleted").ne(true));
        query.fields().include("_id", "name");
        List<GroupInfoDto> existingGroups = findAllDto(query, user);

        // 构建名称到分组的映射
        Map<String, GroupInfoDto> existingMap = existingGroups.stream()
                .collect(Collectors.toMap(GroupInfoDto::getName, g -> g, (g1, g2) -> g1));

        // 处理重名分组
        for (GroupInfoDto groupInfo : groupInfos) {
            if (groupInfo == null || StringUtils.isBlank(groupInfo.getName())) {
                continue;
            }
            GroupInfoDto existing = existingMap.get(groupInfo.getName());
            if (existing != null) {
                String backupName = groupInfo.getName() + GroupConstants.BACKUP_NAME_SEPARATOR
                        + System.currentTimeMillis();
                updateById(existing.getId(), Update.update("name", backupName), user);
                renamedGroups.put(groupInfo.getName(), backupName);
                log.info("Duplicate group name handled, originalName={}, backupName={}", groupInfo.getName(),
                        backupName);
            }
        }
        return renamedGroups;
    }

    protected List<DataSourceConnectionDto> loadConnections(Set<String> connectionIds) {
        if (CollectionUtils.isEmpty(connectionIds)) {
            return new ArrayList<>();
        }
        return dataSourceService.findAllByIds(new ArrayList<>(connectionIds));
    }

    protected String buildGroupExportFileName(List<GroupInfoDto> groupInfos, String yyyymmdd) {
        if (CollectionUtils.size(groupInfos) == 1) {
            GroupInfoDto groupInfo = groupInfos.get(0);
            if (groupInfo != null && StringUtils.isNotBlank(groupInfo.getName())) {
                return groupInfo.getName() + "-" + yyyymmdd;
            }
        }
        return "group_batch" + "-" + yyyymmdd;
    }

    /**
     * 按资源类型提取资源 ID
     *
     * @param groupInfos 分组列表
     * @return 资源类型到 ID 集合的映射
     */
    protected Map<ResourceType, Set<String>> extractResourceIdsByType(List<GroupInfoDto> groupInfos) {
        Map<ResourceType, Set<String>> result = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(groupInfos)) {
            return result;
        }
        for (GroupInfoDto groupInfo : groupInfos) {
            if (CollectionUtils.isEmpty(groupInfo.getResourceItemList())) {
                continue;
            }
            for (ResourceItem item : groupInfo.getResourceItemList()) {
                if (item != null && item.getType() != null && StringUtils.isNotBlank(item.getId())) {
                    result.computeIfAbsent(item.getType(), k -> new LinkedHashSet<>()).add(item.getId());
                }
            }
        }
        return result;
    }

    /**
     * 获取资源 ID
     */
    protected String getResourceId(Object resource) {
        if (resource instanceof TaskDto) {
            ObjectId id = ((TaskDto) resource).getId();
            return id == null ? null : id.toHexString();
        } else if (resource instanceof ModulesDto) {
            ObjectId id = ((ModulesDto) resource).getId();
            return id == null ? null : id.toHexString();
        }
        return null;
    }

    /**
     * 获取资源名称
     */
    protected String getResourceName(Object resource) {
        if (resource instanceof TaskDto) {
            return ((TaskDto) resource).getName();
        } else if (resource instanceof ModulesDto) {
            return ((ModulesDto) resource).getName();
        }
        return null;
    }

    /**
     * 构建导入记录详情（新版本，使用资源处理器）
     */
    protected List<GroupInfoRecordDetail> buildImportRecordDetails(List<GroupInfoDto> groupInfos, Map<ResourceType, Map<String, ?>> resourceMapsByType){
        List<GroupInfoRecordDetail> details = new ArrayList<>();
        for (GroupInfoDto groupInfo : groupInfos) {
            GroupInfoRecordDetail detail = new GroupInfoRecordDetail();
            if (groupInfo.getId() != null) {
                detail.setGroupId(groupInfo.getId().toHexString());
            }
            detail.setGroupName(groupInfo.getName());
            fillRecordDetails(detail, groupInfo, resourceMapsByType, GroupInfoRecordDetail.RecordAction.IMPORTING);
            details.add(detail);
        }
        return details;
    }


    /**
     * 填充记录详情
     */
    protected void fillRecordDetails(GroupInfoRecordDetail detail, GroupInfoDto groupInfo,
                                     Map<ResourceType, Map<String, ?>> resourceMapsByType,
                                     GroupInfoRecordDetail.RecordAction successAction) {
        List<ResourceItem> items = groupInfo.getResourceItemList();
        List<String> resetTaskList = groupInfo.getResetTaskList();
        if (detail == null || CollectionUtils.isEmpty(items)) {
            return;
        }

        for (ResourceItem item : items) {
            if (item == null || StringUtils.isBlank(item.getId()) || item.getType() == null) {
                continue;
            }
            GroupInfoRecordDetail.RecordDetail recordDetail = new GroupInfoRecordDetail.RecordDetail();
            recordDetail.setResourceType(item.getType());
            recordDetail.setResourceId(item.getId());
            if(resetTaskList != null && resetTaskList.contains(item.getId())) {
                recordDetail.setReset(true);
            }
            // 通过资源处理器解析资源名称
            ResourceHandler handler = resourceHandlerRegistry.getHandler(item.getType());
            String resourceName = null;
            if (handler != null) {
                Map<String, ?> resourceMap = resourceMapsByType.get(item.getType());
                if (resourceMap != null) {
                    resourceName = handler.resolveResourceName(item.getId(), resourceMap);
                }
            }
            recordDetail.setResourceName(resourceName);

            if (resourceName == null) {
                recordDetail.setAction(GroupInfoRecordDetail.RecordAction.ERRORED);
                recordDetail.setMessage("missing");
            } else {
                recordDetail.setAction(successAction);
            }
            detail.getRecordDetails().add(recordDetail);
        }
    }

	public String lastestTagName(String groupId) {
		GroupInfoDto groupInfoDto = findById(new ObjectId(groupId));
		if (null == groupInfoDto) {
			throw new IllegalArgumentException(String.format("Group not exists: %s", groupId));
		}
		GroupGitInfoDto gitInfo = groupInfoDto.getGitInfo();
		if (null == gitInfo) {
			throw new IllegalArgumentException("The project does not have a git repository set up");
		}
		GitService gitService = gitServiceRouter.route(gitInfo);
		if (gitService instanceof GitBaseService) {
			return ((GitBaseService) gitService).lastestTagName(gitInfo);
		} else {
			throw new UnsupportedOperationException(gitInfo.toString());
		}
	}
}
