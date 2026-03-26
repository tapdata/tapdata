package com.tapdata.tm.group.service;

import com.mongodb.ConnectionString;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import cn.hutool.core.date.DateUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
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
import com.tapdata.tm.group.vo.*;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.batchup.BatchUpChecker;
import com.tapdata.tm.task.utils.TaskConfigCompareUtil;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
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
import org.springframework.web.multipart.MultipartFile;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.servlet.http.HttpServletResponse;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.data.mongodb.core.query.Update;

@Service
@Slf4j
@Setter(onMethod_ = { @Autowired })
public class GroupInfoService extends BaseService<GroupInfoDto, GroupInfoEntity, ObjectId, GroupInfoRepository> {

    /**
     * 用于导出文件的 Jackson ObjectMapper：
     * - INDENT_OUTPUT：格式化缩进，便于人工阅读和 diff
     * - SORT_PROPERTIES_ALPHABETICALLY + ORDER_MAP_ENTRIES_BY_KEYS：键排序固定，
     *   保证同样内容每次序列化结果一致，PR diff 只体现真实变更
     */
    private static final ObjectMapper EXPORT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    /**
     * 用于对比差异的 ObjectMapper：字段名字母排序 + 忽略 null 值，
     * 确保文件侧（NON_NULL 导出，无 null 字段）与 DB 侧（含 null 字段）在对比前归一化一致。
     */
    private static final ObjectMapper COMPARISON_MAPPER = new ObjectMapper()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);

    /** 所有集合通用的时间戳/操作人字段，每次保存都会变，导出时统一剔除避免 PR diff 噪音 */
    private static final Set<String> COMMON_VOLATILE_FIELDS = new HashSet<>(Arrays.asList(
            "last_updated", "lastUpdate", "last_user_name"
    ));

    /** 导出连接时需剔除的运行时/环境状态字段 */
    private static final Set<String> CONNECTION_EXPORT_EXCLUDED_FIELDS = new HashSet<>(Arrays.asList(
            "loadSchemaField", "loadSchemaTime", "testTime", "transformed", "alarmInfo", "status"
    ));

    /** 导出任务时需剔除的运行时状态字段 */
    private static final Set<String> TASK_EXPORT_EXCLUDED_FIELDS = new HashSet<>(Arrays.asList(
            "currentEventTimestamp", "delayTime", "lastStartDate", "taskRecordId, editVersion",
			"pageVersion", "transformUuid", "transformed", "taskRecordId"
    ));

    /** 导出校验任务时需剔除的字段（空值无意义字段） */
    private static final Set<String> INSPECT_EXPORT_EXCLUDED_FIELDS = new HashSet<>(Arrays.asList(
            "byFirstCheckId"
    ));

    /**
     * BaseDto 中属于运行时环境的字段，嵌套子文档（如 gitInfo）导出时同样剔除，
     * 防止 MongoDB subdocument 的 id / 时间戳在每次保存后引起 PR diff 噪音。
     */
    private static final Set<String> BASE_DTO_VOLATILE_FIELDS = new HashSet<>(Arrays.asList(
            "id", "customId", "createTime", "last_updated", "user_id", "lastUpdBy", "createUser", "permissionActions"
    ));

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
    private MetadataDefinitionService metadataDefinitionService;
    private BatchUpChecker batchUpChecker;

    @Override
    protected void beforeSave(GroupInfoDto dto, UserDetail userDetail) {
		checkGitInfoAndHandleUrl(dto);
		checkResourceExclusiveness(dto, null);
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
		checkResourceExclusiveness(dto, dto.getId());
		return super.update(query, dto);
	}

	/**
	 * 校验 resourceItemList 中的 task/api 资源是否已存在于其他 group。
	 * MIGRATE_TASK、SYNC_TASK、MODULE 同一时刻只能属于一个 group。
	 *
	 * @param dto            待保存/更新的 GroupInfoDto
	 * @param excludeGroupId 更新时传入当前 group ID 以排除自身；创建时传 null
	 */
	private void checkResourceExclusiveness(GroupInfoDto dto, ObjectId excludeGroupId) {
		if (CollectionUtils.isEmpty(dto.getResourceItemList())) {
			return;
		}

		Set<ResourceType> exclusiveTypes = EnumSet.of(
				ResourceType.MIGRATE_TASK, ResourceType.SYNC_TASK, ResourceType.MODULE);

		// 保留 id -> type 映射，供后续查名称时判断用哪个服务
		Map<String, ResourceType> idToType = dto.getResourceItemList().stream()
				.filter(item -> item != null && exclusiveTypes.contains(item.getType())
						&& StringUtils.isNotBlank(item.getId()))
				.collect(Collectors.toMap(ResourceItem::getId, ResourceItem::getType, (a, b) -> a));

		if (idToType.isEmpty()) {
			return;
		}

		Criteria criteria = Criteria.where("resourceItemList.id").in(idToType.keySet())
				.and("is_deleted").ne(true);
		if (excludeGroupId != null) {
			criteria = criteria.and("_id").ne(excludeGroupId);
		}

		Query query = new Query(criteria).limit(1);
		GroupInfoDto conflictGroup = findOne(query);
		if (conflictGroup == null) {
			return;
		}

		Set<String> conflictGroupResourceIds = conflictGroup.getResourceItemList().stream()
				.filter(item -> item != null && StringUtils.isNotBlank(item.getId()))
				.map(ResourceItem::getId)
				.collect(Collectors.toSet());

		String conflictResourceId = idToType.keySet().stream()
				.filter(conflictGroupResourceIds::contains)
				.findFirst()
				.orElse("unknown");

		// 查询冲突资源的名称，给出更友好的错误提示
		String resourceName = resolveResourceName(conflictResourceId, idToType.get(conflictResourceId));

		throw new BizException("Group.Resource.AlreadyInGroup", resourceName, conflictGroup.getName());
	}

	private String resolveResourceName(String resourceId, ResourceType type) {
		try {
			if (type == ResourceType.MODULE) {
				List<ModulesDto> modules = modulesService.findAllModulesByIds(List.of(resourceId));
				if (CollectionUtils.isNotEmpty(modules) && StringUtils.isNotBlank(modules.get(0).getName())) {
					return modules.get(0).getName() + "(" + resourceId + ")";
				}
			} else if (type == ResourceType.MIGRATE_TASK || type == ResourceType.SYNC_TASK) {
				List<TaskDto> tasks = taskService.findAllTasksByIds(List.of(resourceId));
				if (CollectionUtils.isNotEmpty(tasks) && StringUtils.isNotBlank(tasks.get(0).getName())) {
					return tasks.get(0).getName() + "(" + resourceId + ")";
				}
			}
		} catch (Exception e) {
			log.warn("Failed to resolve resource name for id={}, type={}", resourceId, type, e);
		}
		return resourceId;
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
        // 查找 MIGRATE_TASK 和 SYNC_TASK 关联的校验任务，添加到 resourceItemList
//        Set<String> taskIds = new HashSet<>();
//        for (GroupInfoDto groupInfoDto : groupInfoDtoPage.getItems()) {
//            if (CollectionUtils.isNotEmpty(groupInfoDto.getResourceItemList())) {
//                for (ResourceItem item : groupInfoDto.getResourceItemList()) {
//                    if (item != null && item.getId() != null &&
//                            (ResourceType.MIGRATE_TASK.equals(item.getType()) || ResourceType.SYNC_TASK.equals(item.getType()))) {
//                        taskIds.add(item.getId());
//                    }
//                }
//            }
//        }
//        if (CollectionUtils.isNotEmpty(taskIds)) {
//            List<InspectDto> relatedInspects = inspectService.findByTaskIdList(new ArrayList<>(taskIds));
//            if (CollectionUtils.isNotEmpty(relatedInspects)) {
//                Map<String, List<InspectDto>> inspectsByFlowId = relatedInspects.stream()
//                        .filter(i -> i.getFlowId() != null && i.getId() != null)
//                        .collect(Collectors.groupingBy(InspectDto::getFlowId));
//                for (GroupInfoDto groupInfoDto : groupInfoDtoPage.getItems()) {
//                    if (CollectionUtils.isEmpty(groupInfoDto.getResourceItemList())) {
//                        continue;
//                    }
//                    Set<String> existingInspectIds = groupInfoDto.getResourceItemList().stream()
//                            .filter(item -> item != null && ResourceType.INSPECT_TASK.equals(item.getType()) && item.getId() != null)
//                            .map(ResourceItem::getId)
//                            .collect(Collectors.toSet());
//                    List<ResourceItem> newItems = new ArrayList<>();
//                    for (ResourceItem item : groupInfoDto.getResourceItemList()) {
//                        if (item == null || item.getId() == null) continue;
//                        if (!ResourceType.MIGRATE_TASK.equals(item.getType()) && !ResourceType.SYNC_TASK.equals(item.getType())) continue;
//                        List<InspectDto> inspects = inspectsByFlowId.get(item.getId());
//                        if (CollectionUtils.isEmpty(inspects)) continue;
//                        for (InspectDto inspectDto : inspects) {
//                            String inspectId = inspectDto.getId().toHexString();
//                            if (!existingInspectIds.contains(inspectId)) {
//                                ResourceItem newItem = new ResourceItem();
//                                newItem.setId(inspectId);
//                                newItem.setType(ResourceType.INSPECT_TASK);
//                                newItem.setName(inspectDto.getName());
//                                newItems.add(newItem);
//                                existingInspectIds.add(inspectId);
//                            }
//                        }
//                    }
//                    if (CollectionUtils.isNotEmpty(newItems)) {
//                        groupInfoDto.getResourceItemList().addAll(newItems);
//                    }
//                }
//            }
//        }
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
            query.addCriteria(resourceCriteria);
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
                handler.handleRelatedResources(payloadsByType, entry.getValue(), user,new HashSet<>());
            }
        }
        for (Map.Entry<ResourceType, List<?>> entry : resourcesByType.entrySet()) {
            ResourceHandler handler = resourceHandlerRegistry.getHandler(entry.getKey());
            if (handler != null) {
                List<TaskUpAndLoadDto> payload = handler.buildExportPayload(entry.getValue(), user);
                payloadsByType.put(entry.getKey().name(), payload);
            }
        }
        // 导出连接关联的模型元数据（database 级别 + table/collection 级别，不含任务派生的模型）
        Set<String> exportConnectionIds = new HashSet<>();
        for (Map.Entry<ResourceType, List<?>> entry : resourcesByType.entrySet()) {
            ResourceHandler handler = resourceHandlerRegistry.getHandler(entry.getKey());
            if (handler != null) {
                handler.loadConnections(entry.getValue()).stream()
                        .filter(c -> c.getId() != null)
                        .forEach(c -> exportConnectionIds.add(c.getId().toHexString()));
            }
        }
        if (!exportConnectionIds.isEmpty()) {
            List<TaskUpAndLoadDto> connMetadataPayload = buildConnectionMetadataPayload(exportConnectionIds, user);
            if (!connMetadataPayload.isEmpty()) {
                payloadsByType.computeIfAbsent(ResourceType.CONNECTION.name(), k -> new ArrayList<>())
                        .addAll(connMetadataPayload);
                log.info("Exported connection metadata, connectionCount={}, metadataCount={}",
                        exportConnectionIds.size(), connMetadataPayload.size());
            }
        }

        // 构建资源 id → name 映射，用于在 GroupInfo 的 resourceItemList 中补充资源名称
        Map<String, String> resourceIdToName = buildResourceIdToNameMap(payloadsByType);
        List<TaskUpAndLoadDto> groupInfoPayload = buildGroupInfoPayload(groupInfos, resourceIdToName);

        // 构建导出记录
        String yyyymmdd = DateUtil.today().replaceAll("-", "");
        String name = buildGroupExportFileName(groupInfos, yyyymmdd);
        GroupInfoRecordDto recordDto = buildExportRecord(GroupInfoRecordDto.TYPE_EXPORT, user,
                buildExportRecordDetails(groupInfos, resourcesByType), name, exportGroupRequest, groupInfos.get(0));
        recordDto = groupInfoRecordService.save(recordDto, user);

        // 构建导出文件内容
        Map<String, byte[]> contents = buildExportContents(groupInfoPayload, payloadsByType);

        log.info("Start exporting groups, groupCount={}, user={}", groupInfos.size(), user.getUsername());

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
			strategy.exportGroups(new GroupExportRequest(response, contents, name, groupInfos.get(0),
						exportGroupRequest.getGitTag(),
						exportGroupRequest.getGitBranchName(),
						exportGroupRequest.getGitPrTitle(),
						exportGroupRequest.getGitPrDescription(),
						recordDto));
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
				if (e instanceof BizException) {
					throw (BizException) e;
				}
				throw new BizException("Group.Export.Error", e.getMessage());
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

    // ====================== Preview (compare) APIs ======================

    public GroupPreviewResult previewImport(MultipartFile file, UserDetail user) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = parseImportPayloads(file);
        GroupPreviewResult result = new GroupPreviewResult();
        result.setConnections(buildConnectionDiff(payloads, user));
        result.setTasks(buildTaskDiff(payloads, user));
        result.setApis(buildApiDiff(payloads, user));
        return result;
    }

    public ResourceDiff previewConnections(MultipartFile file, UserDetail user) throws IOException {
        return buildConnectionDiff(parseImportPayloads(file), user);
    }

    public ResourceDiff previewTasks(MultipartFile file, UserDetail user) throws IOException {
        return buildTaskDiff(parseImportPayloads(file), user);
    }

    public ResourceDiff previewMigrateTasks(MultipartFile file, UserDetail user) throws IOException {
        return buildTaskDiff(filterPayloadsByTaskType(parseImportPayloads(file), "MigrateTask.json"), user);
    }

    public ResourceDiff previewSyncTasks(MultipartFile file, UserDetail user) throws IOException {
        return buildTaskDiff(filterPayloadsByTaskType(parseImportPayloads(file), "SyncTask.json"), user);
    }

    public ResourceDiff previewApis(MultipartFile file, UserDetail user) throws IOException {
        return buildApiDiff(parseImportPayloads(file), user);
    }

    // ====================== Split Import APIs (async) ======================

    public GroupImportResult importConnections(MultipartFile file, ImportModeEnum importMode,
            UserDetail user, MultipartFile vaultFile, boolean sync) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = parseImportPayloads(file);
        ResourceDiff diff = buildConnectionDiff(payloads, user);
        String fileName = file.getOriginalFilename();
        // 优先使用单独上传的 vault 文件；若未传，则尝试从 tar 包内的 vault.json 读取
        Map<String, String> vaultSecrets = parseVaultFile(vaultFile);
        if (vaultSecrets.isEmpty()) {
            vaultSecrets = parseVaultSecrets(payloads);
        }
        GroupInfoRecordDto recordDto = buildRecord(GroupInfoRecordDto.TYPE_IMPORT, user, new ArrayList<>(), fileName);
        recordDto.setProgress(0);
        recordDto = groupInfoRecordService.save(recordDto, user);
        ObjectId recordId = recordDto.getId();
        ImportModeEnum effectiveMode = importMode != null ? importMode : ImportModeEnum.GROUP_IMPORT;
        if (sync) {
            // 直接调用，绕过 Spring AOP 代理，@Async 不生效，同步执行
            executeImportConnectionsAsync(payloads, effectiveMode, user, recordId, vaultSecrets);
        } else {
            GroupInfoService self = SpringContextHelper.getBean(GroupInfoService.class);
            self.executeImportConnectionsAsync(payloads, effectiveMode, user, recordId, vaultSecrets);
        }
        return new GroupImportResult(recordId.toHexString(), diff);
    }


    public GroupImportResult importTasks(MultipartFile file, ImportModeEnum importMode, UserDetail user,
            boolean sync) throws IOException {
        return doImportTasks(parseImportPayloads(file), file.getOriginalFilename(), importMode, user, sync);
    }

    public GroupImportResult importMigrateTasks(MultipartFile file, ImportModeEnum importMode, UserDetail user,
            boolean sync) throws IOException {
        return doImportTasks(filterPayloadsByTaskType(parseImportPayloads(file), "MigrateTask.json"),
                file.getOriginalFilename(), importMode, user, sync);
    }

    public GroupImportResult importSyncTasks(MultipartFile file, ImportModeEnum importMode, UserDetail user,
            boolean sync) throws IOException {
        return doImportTasks(filterPayloadsByTaskType(parseImportPayloads(file), "SyncTask.json"),
                file.getOriginalFilename(), importMode, user, sync);
    }

    private GroupImportResult doImportTasks(Map<String, List<TaskUpAndLoadDto>> payloads,
            String fileName, ImportModeEnum importMode, UserDetail user, boolean sync) {
        ResourceDiff diff = buildTaskDiff(payloads, user);
        GroupInfoRecordDto recordDto = buildRecord(GroupInfoRecordDto.TYPE_IMPORT, user, new ArrayList<>(), fileName);
        recordDto.setProgress(0);
        recordDto = groupInfoRecordService.save(recordDto, user);
        ObjectId recordId = recordDto.getId();
        ImportModeEnum effectiveMode = importMode != null ? importMode : ImportModeEnum.GROUP_IMPORT;
        if (sync) {
            executeImportTasksStandaloneAsync(payloads, effectiveMode, user, recordId);
        } else {
            GroupInfoService self = SpringContextHelper.getBean(GroupInfoService.class);
            self.executeImportTasksStandaloneAsync(payloads, effectiveMode, user, recordId);
        }
        return new GroupImportResult(recordId.toHexString(), diff);
    }

    public ObjectId importApisByNames(MultipartFile file, List<String> names,
            ImportModeEnum importMode, UserDetail user) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = parseImportPayloads(file);
        String fileName = file.getOriginalFilename();
        GroupInfoRecordDto recordDto = buildRecord(GroupInfoRecordDto.TYPE_IMPORT, user, new ArrayList<>(), fileName);
        recordDto.setProgress(0);
        recordDto = groupInfoRecordService.save(recordDto, user);
        ObjectId recordId = recordDto.getId();
        GroupInfoService self = SpringContextHelper.getBean(GroupInfoService.class);
        self.executeImportApisAsync(payloads, names, importMode != null ? importMode : ImportModeEnum.GROUP_IMPORT, user, recordId);
        return recordId;
    }

    public GroupImportResult importApis(MultipartFile file, ImportModeEnum importMode, UserDetail user,
            boolean sync) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = parseImportPayloads(file);
        ResourceDiff diff = buildApiDiff(payloads, user);
        String fileName = file.getOriginalFilename();
        GroupInfoRecordDto recordDto = buildRecord(GroupInfoRecordDto.TYPE_IMPORT, user, new ArrayList<>(), fileName);
        recordDto.setProgress(0);
        recordDto = groupInfoRecordService.save(recordDto, user);
        ObjectId recordId = recordDto.getId();
        ImportModeEnum effectiveMode = importMode != null ? importMode : ImportModeEnum.REPLACE;
        if (sync) {
            executeImportApisStandaloneAsync(payloads, effectiveMode, user, recordId);
        } else {
            GroupInfoService self = SpringContextHelper.getBean(GroupInfoService.class);
            self.executeImportApisStandaloneAsync(payloads, effectiveMode, user, recordId);
        }
        return new GroupImportResult(recordId.toHexString(), diff);
    }

    /**
     * 同步导入 GroupInfo：从文件（tar 或 json）中解析 GroupInfo.json，
     * 按资源类型+名称在 DB 中查找当前资源 ID，然后以 name 为 key upsert 各分组。
     */
    public ObjectId importGroupInfo(MultipartFile file, ImportModeEnum importMode, UserDetail user) throws IOException {
        Map<String, List<TaskUpAndLoadDto>> payloads = parseImportPayloads(file);
        String fileName = file.getOriginalFilename();

        List<TaskUpAndLoadDto> groupPayload = payloads.getOrDefault("GroupInfo.json", Collections.emptyList());
        List<GroupInfoDto> groupInfos = new ArrayList<>();
        collectGroupInfoPayload(groupPayload, groupInfos);

        if (CollectionUtils.isEmpty(groupInfos)) {
            throw new BizException("GroupInfo.Not.Found");
        }

        GroupInfoRecordDto recordDto = buildRecord(GroupInfoRecordDto.TYPE_IMPORT, user, new ArrayList<>(), fileName);
        recordDto.setProgress(0);
        recordDto = groupInfoRecordService.save(recordDto, user);
        ObjectId recordId = recordDto.getId();

        try {
            for (GroupInfoDto groupInfo : groupInfos) {
                List<ResourceItem> resolved = resolveResourceItemIds(groupInfo.getResourceItemList(), user);
                groupInfo.setResourceItemList(resolved);
                groupInfo.setCreateUser(null);
                groupInfo.setCustomId(null);
                groupInfo.setLastUpdBy(null);
                groupInfo.setUserId(null);
                groupInfo.setId(null);
                upsertByWhere(Where.where("name", groupInfo.getName()), groupInfo, user);
            }
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
        } catch (Exception e) {
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), new ArrayList<>(), user);
            throw e;
        }
        return recordId;
    }

    /**
     * 根据资源类型+名称在 DB 中查找当前资源 ID，返回解析后的 ResourceItem 列表。
     * 若 name 字段为空则保留原 id 不变。
     */
    private List<ResourceItem> resolveResourceItemIds(List<ResourceItem> items, UserDetail user) {
        if (CollectionUtils.isEmpty(items)) {
            return new ArrayList<>();
        }

        // Collect names per type
        Set<String> taskNames = new HashSet<>();
        Set<String> connNames = new HashSet<>();
        Set<String> moduleNames = new HashSet<>();
        Set<String> inspectNames = new HashSet<>();
        for (ResourceItem item : items) {
            if (item == null || item.getType() == null || StringUtils.isBlank(item.getName())) continue;
            switch (item.getType()) {
                case MIGRATE_TASK, SYNC_TASK, SHARE_CACHE -> taskNames.add(item.getName());
                case CONNECTION -> connNames.add(item.getName());
                case MODULE -> moduleNames.add(item.getName());
                case INSPECT_TASK -> inspectNames.add(item.getName());
                default -> {}
            }
        }

        // Query DB for each type and build name→id maps
        Map<String, String> taskNameToId = new HashMap<>();
        if (!taskNames.isEmpty()) {
            taskService.findAllDto(new Query(Criteria.where("name").in(taskNames).and("is_deleted").ne(true)), user)
                    .forEach(t -> taskNameToId.putIfAbsent(t.getName(), t.getId().toHexString()));
        }
        Map<String, String> connNameToId = new HashMap<>();
        if (!connNames.isEmpty()) {
            dataSourceService.findAllDto(new Query(Criteria.where("name").in(connNames).and("is_deleted").ne(true)), user)
                    .forEach(c -> connNameToId.putIfAbsent(c.getName(), c.getId().toHexString()));
        }
        Map<String, String> moduleNameToId = new HashMap<>();
        if (!moduleNames.isEmpty()) {
            modulesService.findAllDto(new Query(Criteria.where("name").in(moduleNames).and("is_deleted").ne(true)), user)
                    .forEach(m -> moduleNameToId.putIfAbsent(m.getName(), m.getId().toHexString()));
        }
        Map<String, String> inspectNameToId = new HashMap<>();
        for (String name : inspectNames) {
            List<InspectDto> found = inspectService.findByName(name);
            if (CollectionUtils.isNotEmpty(found) && found.get(0).getId() != null) {
                inspectNameToId.putIfAbsent(name, found.get(0).getId().toHexString());
            }
        }

        // Resolve each item
        List<ResourceItem> resolved = new ArrayList<>();
        for (ResourceItem item : items) {
            if (item == null || item.getType() == null) continue;
            ResourceItem copy = new ResourceItem();
            copy.setType(item.getType());
            String resolvedId = null;
            if (StringUtils.isNotBlank(item.getName())) {
                resolvedId = switch (item.getType()) {
                    case MIGRATE_TASK, SYNC_TASK, SHARE_CACHE -> taskNameToId.get(item.getName());
                    case CONNECTION -> connNameToId.get(item.getName());
                    case MODULE -> moduleNameToId.get(item.getName());
                    case INSPECT_TASK -> inspectNameToId.get(item.getName());
                    default -> null;
                };
            }
            copy.setId(StringUtils.isNotBlank(resolvedId) ? resolvedId : item.getId());
            resolved.add(copy);
        }
        return resolved;
    }

    @Async
    public void executeImportConnectionsAsync(Map<String, List<TaskUpAndLoadDto>> payloads,
            ImportModeEnum importMode, UserDetail user, ObjectId recordId, Map<String, String> vaultSecrets) {
        log.info("Async import connections started, recordId={}", recordId);
        try {
            // 参照 buildConnectionDiff：以 name 为 key，用 parseConnectionDto 解析（带异常保护）
            Map<String, DataSourceConnectionDto> connections = new LinkedHashMap<>();
            List<MetadataInstancesDto> connectionMetadata = new ArrayList<>();
            for (TaskUpAndLoadDto item : payloads.getOrDefault("Connection.json", Collections.emptyList())) {
                if (GroupConstants.COLLECTION_CONNECTION.equals(item.getCollectionName())) {
                    DataSourceConnectionDto conn = parseConnectionDto(item.getJson());
                    if (conn != null && StringUtils.isNotBlank(conn.getName())) {
                        connections.putIfAbsent(conn.getName(), conn);
                    }
                } else if (GroupConstants.COLLECTION_METADATA_INSTANCES.equals(item.getCollectionName())) {
                    MetadataInstancesDto metadata = JsonUtil.parseJsonUseJackson(item.getJson(), MetadataInstancesDto.class);
                    if (metadata != null) {
                        connectionMetadata.add(metadata);
                    }
                }
            }
            // 参照 executeImportAsync Stage 1：校验、vault 注入、batchImport
            try {
                batchUpChecker.checkDataSourceConnection(new ArrayList<>(connections.values()), user, false);
            } catch (Exception e) {
                updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), null, user);
                throw new RuntimeException(e);
            }
            // 导入 MetadataDefinition（标签），并更新 connections 的 listtags ID 映射
            Map<String, String> tagMap = importMetadataDefinitionsAndGetTagMap(payloads, user);
            if (!tagMap.isEmpty()) {
                Map<ResourceType, Map<String, ?>> connResourceMap = new LinkedHashMap<>();
                connResourceMap.put(ResourceType.CONNECTION, connections);
                checkTags(connResourceMap, tagMap);
            }
            if (!vaultSecrets.isEmpty()) {
                log.info("Vault secrets found, injecting into {} connections", connections.size());
                injectVaultSecrets(connections, vaultSecrets, user);
            }
            refreshImportLastUpdate(connections.values(), connectionMetadata);
            Map<String, DataSourceConnectionDto> conMap = dataSourceService.batchImport(
                    new ArrayList<>(connections.values()), user, ImportModeEnum.REPLACE);
            metadataInstancesService.batchImport(connectionMetadata, user, conMap, new HashMap<>(), new HashMap<>());
            log.info("Async import connections completed, recordId={}, count={}", recordId, connections.size());
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
        } catch (Exception e) {
            log.error("Async import connections failed, recordId={}, error={}",
                    recordId, ThrowableUtils.getStackTraceByPn(e));
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getStackTrace(e), new ArrayList<>(), user);
            // 同步调用时（this 直接调用，绕过 @Async 代理）异常会传播给 HTTP 调用方；
            // 异步调用时（经由 Spring proxy）异常由 AsyncUncaughtExceptionHandler 静默处理。
            throw new BizException("Group.Import.Failed", e.getMessage());
        }
    }

    @Async
    public void executeImportTasksAsync(Map<String, List<TaskUpAndLoadDto>> payloads, List<String> names,
            ImportModeEnum importMode, UserDetail user, ObjectId recordId) {
        log.info("Async import tasks started, recordId={}", recordId);
        try {
            Map<ResourceType, Map<String, ?>> resourceMapsByType = new LinkedHashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataByType = new LinkedHashMap<>();
            // collect tasks via handlers
            for (ResourceType type : List.of(ResourceType.MIGRATE_TASK, ResourceType.SYNC_TASK, ResourceType.INSPECT_TASK)) {
                ResourceHandler handler = resourceHandlerRegistry.getHandler(type);
                if (handler != null) {
                    String filename = ResourceType.getResourceName(type.name());
                    List<TaskUpAndLoadDto> payload = payloads.getOrDefault(filename, Collections.emptyList());
                    Map<String, Object> resourceMap = new LinkedHashMap<>();
                    List<MetadataInstancesDto> metadata = new ArrayList<>();
                    handler.collectPayload(payload, resourceMap, metadata);
                    resourceMapsByType.put(type, resourceMap);
                    metadataByType.put(type, metadata);
                    handler.collectPayloadRelatedResources(payloads, resourceMapsByType, metadataByType, user);
                }
            }
            Map<String, TaskDto> migrateTasks = (Map<String, TaskDto>) resourceMapsByType.getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyMap());
            Map<String, TaskDto> syncTasks = (Map<String, TaskDto>) resourceMapsByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyMap());
            Map<String, TaskDto> shareCacheTasks = (Map<String, TaskDto>) resourceMapsByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyMap());
            Map<String, InspectDto> inspectTasks = (Map<String, InspectDto>) resourceMapsByType.getOrDefault(ResourceType.INSPECT_TASK, Collections.emptyMap());
            // filter by names
            if (CollectionUtils.isNotEmpty(names)) {
                migrateTasks.entrySet().removeIf(e -> !names.contains(e.getValue().getName()));
                syncTasks.entrySet().removeIf(e -> !names.contains(e.getValue().getName()));
                shareCacheTasks.entrySet().removeIf(e -> !names.contains(e.getValue().getName()));
                inspectTasks.entrySet().removeIf(e -> !names.contains(e.getValue().getName()));
            }
            List<TaskDto> allTasks = new ArrayList<>();
            allTasks.addAll(migrateTasks.values());
            allTasks.addAll(syncTasks.values());
            allTasks.addAll(shareCacheTasks.values());
            // Build conMap from existing DB connections
            Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
            Map<String, String> taskIdMap = new HashMap<>();
            Map<String, String> nodeIdMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(allTasks)) {
                refreshBaseDtoLastUpdate(allTasks);
                taskService.batchImport(allTasks, user, importMode, new ArrayList<>(), conMap, taskIdMap, nodeIdMap, new ArrayList<>());
                List<MetadataInstancesDto> allTaskMetadata = new ArrayList<>();
                allTaskMetadata.addAll(metadataByType.getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyList()));
                allTaskMetadata.addAll(metadataByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyList()));
                allTaskMetadata.addAll(metadataByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyList()));
                refreshMetadataLastUpdate(allTaskMetadata);
                metadataInstancesService.batchImport(allTaskMetadata, user, conMap, taskIdMap, nodeIdMap);
            }
            if (MapUtils.isNotEmpty(inspectTasks)) {
                refreshBaseDtoLastUpdate(inspectTasks.values());
                Map<String, String> taskIdToNameMap = buildTaskIdToNameMap(migrateTasks, syncTasks);
                inspectService.importTaskByGroup(new ArrayList<>(inspectTasks.values()), taskIdMap, taskIdToNameMap, conMap, user);
            }
            log.info("Async import tasks completed, recordId={}, taskCount={}, inspectCount={}", recordId, allTasks.size(), inspectTasks.size());
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
        } catch (Exception e) {
            log.error("Async import tasks failed, recordId={}", recordId, e);
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), new ArrayList<>(), user);
        }
    }

    @Async
    public void executeImportApisAsync(Map<String, List<TaskUpAndLoadDto>> payloads, List<String> names,
            ImportModeEnum importMode, UserDetail user, ObjectId recordId) {
        log.info("Async import apis started, recordId={}", recordId);
        try {
            Map<ResourceType, Map<String, ?>> resourceMapsByType = new LinkedHashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataByType = new LinkedHashMap<>();
            ResourceHandler moduleHandler = resourceHandlerRegistry.getHandler(ResourceType.MODULE);
            if (moduleHandler != null) {
                String filename = ResourceType.getResourceName(ResourceType.MODULE.name());
                List<TaskUpAndLoadDto> payload = payloads.getOrDefault(filename, Collections.emptyList());
                Map<String, Object> resourceMap = new LinkedHashMap<>();
                List<MetadataInstancesDto> metadata = new ArrayList<>();
                moduleHandler.collectPayload(payload, resourceMap, metadata);
                resourceMapsByType.put(ResourceType.MODULE, resourceMap);
                metadataByType.put(ResourceType.MODULE, metadata);
                moduleHandler.collectPayloadRelatedResources(payloads, resourceMapsByType, metadataByType, user);
            }
            Map<String, ModulesDto> modules = (Map<String, ModulesDto>) resourceMapsByType.getOrDefault(ResourceType.MODULE, Collections.emptyMap());
            if (CollectionUtils.isNotEmpty(names)) {
                modules.entrySet().removeIf(e -> !names.contains(e.getValue().getName()));
            }
            if (MapUtils.isNotEmpty(modules)) {
                refreshModuleLastUpdate(modules.values());
                modulesService.batchImport(new ArrayList<>(modules.values()), user, importMode, new HashMap<>(), null);
                refreshMetadataLastUpdate(metadataByType.getOrDefault(ResourceType.MODULE, Collections.emptyList()));
                metadataInstancesService.batchImport(
                        metadataByType.getOrDefault(ResourceType.MODULE, Collections.emptyList()),
                        user, new HashMap<>(), null, null);
            }
            log.info("Async import apis completed, recordId={}, count={}", recordId, modules.size());
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
        } catch (Exception e) {
            log.error("Async import apis failed, recordId={}", recordId, e);
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), new ArrayList<>(), user);
        }
    }

    @Async
    public void executeImportApisStandaloneAsync(Map<String, List<TaskUpAndLoadDto>> payloads,
            ImportModeEnum importMode, UserDetail user, ObjectId recordId) {
        log.info("Async import apis started, recordId={}", recordId);
        try {
            // 收集文件中所有 modules
            Map<ResourceType, Map<String, ?>> resourceMapsByType = new LinkedHashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataByType = new LinkedHashMap<>();
            ResourceHandler moduleHandler = resourceHandlerRegistry.getHandler(ResourceType.MODULE);
            if (moduleHandler != null) {
                String filename = ResourceType.getResourceName(ResourceType.MODULE.name());
                List<TaskUpAndLoadDto> payload = payloads.getOrDefault(filename, Collections.emptyList());
                Map<String, Object> resourceMap = new LinkedHashMap<>();
                List<MetadataInstancesDto> metadata = new ArrayList<>();
                moduleHandler.collectPayload(payload, resourceMap, metadata);
                resourceMapsByType.put(ResourceType.MODULE, resourceMap);
                metadataByType.put(ResourceType.MODULE, metadata);
            }
            Map<String, ModulesDto> allModules = (Map<String, ModulesDto>) resourceMapsByType
                    .getOrDefault(ResourceType.MODULE, Collections.emptyMap());
            if (MapUtils.isEmpty(allModules)) {
                log.info("Async import apis completed, no modules found, recordId={}", recordId);
                updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
                return;
            }

            // 导入 API 分组（MetadataDefinition），同时更新 modules 的 listtags ID 映射
            Map<String, String> tagMap = importMetadataDefinitionsAndGetTagMap(payloads, user);
            checkTags(resourceMapsByType, tagMap);

            // 通过 diff 确定哪些 API 有变更（新增 + 有内容变化），只对这些进行导入
            ResourceDiff diff = buildApiDiff(payloads, user);
            Set<String> changedNames = new HashSet<>();
            diff.getAdd().forEach(item -> changedNames.add(item.getName()));
            diff.getUpdate().forEach(item -> changedNames.add(item.getName()));
            log.info("Async import apis: add={}, update={}, total changed={}, recordId={}",
                    diff.getAdd().size(), diff.getUpdate().size(), changedNames.size(), recordId);

            // CONNECTION 没有专用 handler，通过任意已注册 handler 的 collectPayloadRelatedResources 解析连接数据
            resourceHandlerRegistry.getAllHandlers().stream().findFirst().ifPresent(
                    h -> h.collectPayloadRelatedResources(payloads, resourceMapsByType, metadataByType, user));

            // 用 payload 中连接的名称查询 DB 里已存在的连接，建立 旧ID -> 新连接DTO 的映射
            Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
            Map<String, DataSourceConnectionDto> payloadConnections = (Map<String, DataSourceConnectionDto>) resourceMapsByType
                    .getOrDefault(ResourceType.CONNECTION, Collections.emptyMap());
            if (!payloadConnections.isEmpty()) {
                List<String> connectionNames = payloadConnections.values().stream()
                        .map(DataSourceConnectionDto::getName)
                        .filter(StringUtils::isNotBlank)
                        .distinct()
                        .collect(Collectors.toList());
                List<DataSourceConnectionDto> existingConnections = dataSourceService.findAllDto(
                        new Query(Criteria.where("name").in(connectionNames).and("is_deleted").ne(true)), user);
                Map<String, DataSourceConnectionDto> nameToExisting = existingConnections.stream()
                        .collect(Collectors.toMap(DataSourceConnectionDto::getName, c -> c, (a, b) -> a));
                for (Map.Entry<String, DataSourceConnectionDto> entry : payloadConnections.entrySet()) {
                    String oldId = entry.getKey();
                    String name = entry.getValue().getName();
                    DataSourceConnectionDto existing = nameToExisting.get(name);
                    if (existing != null) {
                        conMap.put(oldId, existing);
                        log.info("conMap built (api): oldId={}, name={}, newId={}", oldId, name, existing.getId());
                    } else {
                        log.warn("conMap build (api): connection '{}' (oldId={}) not found in DB", name, oldId);
                    }
                }
                log.info("conMap built from payload connections (api), payload size={}, matched={}", payloadConnections.size(), conMap.size());
            }

            // 只导入有变更的 modules
            List<ModulesDto> toImport = allModules.values().stream()
                    .filter(m -> changedNames.contains(m.getName()))
                    .collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(toImport)) {
                // handleReplaceMode 内部处理：若已有 API 状态为 active → 先 unpublish → 覆盖内容 → republish
                refreshModuleLastUpdate(toImport);
                modulesService.batchImport(toImport, user, importMode, conMap, null);
                refreshMetadataLastUpdate(metadataByType.getOrDefault(ResourceType.MODULE, Collections.emptyList()));
                metadataInstancesService.batchImport(
                        metadataByType.getOrDefault(ResourceType.MODULE, Collections.emptyList()),
                        user, conMap, null, null);
            }

            log.info("Async import apis completed, recordId={}, imported={}", recordId, toImport.size());
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
        } catch (Exception e) {
            log.error("Async import apis failed, recordId={}", recordId, e);
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), new ArrayList<>(), user);
            throw new BizException("Group.Import.Failed", e.getMessage());
        }
    }

    @Async
    public void executeImportTasksStandaloneAsync(Map<String, List<TaskUpAndLoadDto>> payloads,
            ImportModeEnum importMode, UserDetail user, ObjectId recordId) {
        log.info("Async import tasks standalone started, recordId={}", recordId);
        try {
            // 收集文件中所有 tasks
            Map<ResourceType, Map<String, ?>> resourceMapsByType = new LinkedHashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataByType = new LinkedHashMap<>();
            for (ResourceType type : List.of(ResourceType.MIGRATE_TASK, ResourceType.SYNC_TASK, ResourceType.INSPECT_TASK)) {
                ResourceHandler handler = resourceHandlerRegistry.getHandler(type);
                if (handler != null) {
                    String filename = ResourceType.getResourceName(type.name());
                    List<TaskUpAndLoadDto> payload = payloads.getOrDefault(filename, Collections.emptyList());
                    Map<String, Object> resourceMap = new LinkedHashMap<>();
                    List<MetadataInstancesDto> metadata = new ArrayList<>();
                    handler.collectPayload(payload, resourceMap, metadata);
                    resourceMapsByType.put(type, resourceMap);
                    metadataByType.put(type, metadata);
                }
            }
            // CONNECTION 没有专用 handler，通过任意已注册 handler 的 collectPayloadRelatedResources 解析连接数据
            resourceHandlerRegistry.getAllHandlers().stream().findFirst().ifPresent(
                    h -> h.collectPayloadRelatedResources(payloads, resourceMapsByType, metadataByType, user));

            // 导入 MetadataDefinition（标签），并用 tagIdMap 更新任务的 listtags
            Map<String, String> tagMap = importMetadataDefinitionsAndGetTagMap(payloads, user);
            checkTags(resourceMapsByType, tagMap);

            // diff: 只对有变更（add + update）的任务操作，状态字段不参与对比
            ResourceDiff diff = buildTaskDiff(payloads, user);
            Set<String> changedNames = new HashSet<>();
            diff.getAdd().forEach(item -> changedNames.add(item.getName()));
            diff.getUpdate().forEach(item -> changedNames.add(item.getName()));
            log.info("Task diff: add={}, update={}, recordId={}", diff.getAdd().size(), diff.getUpdate().size(), recordId);

            // ---- 普通任务（migrate / sync / shareCache）----
            Map<String, TaskDto> migrateTasks = (Map<String, TaskDto>) resourceMapsByType.getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyMap());
            Map<String, TaskDto> syncTasks    = (Map<String, TaskDto>) resourceMapsByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyMap());
            Map<String, TaskDto> shareCacheTasks = (Map<String, TaskDto>) resourceMapsByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyMap());

            List<TaskDto> toImportTasks = new ArrayList<>();
            for (Map<String, TaskDto> map : List.of(migrateTasks, syncTasks, shareCacheTasks)) {
                map.values().stream()
                        .filter(t -> changedNames.contains(t.getName()))
                        .forEach(toImportTasks::add);
            }

            // 找出需要导入的任务中，当前在 DB 里处于运行状态的任务，导入前先停止
            Set<String> runningStatuses = Set.of(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN);
            List<String> toImportNames = toImportTasks.stream().map(TaskDto::getName).collect(Collectors.toList());
            Map<String, TaskDto> existingTasksByName = Collections.emptyMap();
            if (!toImportNames.isEmpty()) {
                existingTasksByName = taskService
                        .findAllDto(new Query(Criteria.where("name").in(toImportNames).and("is_deleted").ne(true)), user)
                        .stream().collect(Collectors.toMap(TaskDto::getName, t -> t, (a, b) -> a));
            }
            Set<String> stoppedTaskNames = new HashSet<>();
            for (TaskDto existing : existingTasksByName.values()) {
                if (runningStatuses.contains(existing.getStatus())) {
                    log.info("Stopping task '{}' before import, status={}", existing.getName(), existing.getStatus());
                    stopAndWaitTask(existing, user);
                    stoppedTaskNames.add(existing.getName());
                }
            }

            // 用 payload 中连接的名称查询 DB 里已存在的连接，建立 旧ID -> 新连接DTO 的映射
            // GROUP_IMPORT 场景下连接不会重新导入，但任务节点上的 connectionId 是导出时的旧 ID，需要映射到目标环境的新 ID
            Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
            Map<String, DataSourceConnectionDto> payloadConnections = (Map<String, DataSourceConnectionDto>) resourceMapsByType
                    .getOrDefault(ResourceType.CONNECTION, Collections.emptyMap());
            if (!payloadConnections.isEmpty()) {
                List<String> connectionNames = payloadConnections.values().stream()
                        .map(DataSourceConnectionDto::getName)
                        .filter(StringUtils::isNotBlank)
                        .distinct()
                        .collect(Collectors.toList());
                List<DataSourceConnectionDto> existingConnections = dataSourceService.findAllDto(
                        new Query(Criteria.where("name").in(connectionNames).and("is_deleted").ne(true)), user);
                Map<String, DataSourceConnectionDto> nameToExisting = existingConnections.stream()
                        .collect(Collectors.toMap(DataSourceConnectionDto::getName, c -> c, (a, b) -> a));
                for (Map.Entry<String, DataSourceConnectionDto> entry : payloadConnections.entrySet()) {
                    String oldId = entry.getKey();
                    String name = entry.getValue().getName();
                    DataSourceConnectionDto existing = nameToExisting.get(name);
                    if (existing != null) {
                        conMap.put(oldId, existing);
                        log.info("conMap built: oldId={}, name={}, newId={}", oldId, name, existing.getId());
                    } else {
                        log.warn("conMap build: connection '{}' (oldId={}) not found in DB, tasks referencing it may fail", name, oldId);
                    }
                }
                log.info("conMap built from payload connections, payload size={}, matched={}", payloadConnections.size(), conMap.size());
            }

            // 导入变更的普通任务
            Map<String, String> taskIdMap = new HashMap<>();
            Map<String, String> nodeIdMap = new HashMap<>();
            if (!toImportTasks.isEmpty()) {
                refreshBaseDtoLastUpdate(toImportTasks);
                taskService.batchImport(toImportTasks, user, importMode, new ArrayList<>(), conMap, taskIdMap, nodeIdMap, new ArrayList<>());
                List<MetadataInstancesDto> taskMetadata = new ArrayList<>();
                taskMetadata.addAll(metadataByType.getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyList()));
                taskMetadata.addAll(metadataByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyList()));
                taskMetadata.addAll(metadataByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyList()));
                refreshMetadataLastUpdate(taskMetadata);
                metadataInstancesService.batchImport(taskMetadata, user, conMap, taskIdMap, nodeIdMap);
                log.info("Task import completed, count={}", toImportTasks.size());
            }

            // 重启导入前被停止的任务
            for (String name : stoppedTaskNames) {
                TaskDto imported = taskService.findOne(
                        new Query(Criteria.where("name").is(name).and("is_deleted").ne(true)), user);
                if (imported != null) {
                    log.info("Restarting task '{}' after import", name);
                    taskService.start(imported.getId(), user);
                }
            }

            // ---- 校验任务（inspect）----
            Map<String, InspectDto> inspectTasks = (Map<String, InspectDto>) resourceMapsByType.getOrDefault(ResourceType.INSPECT_TASK, Collections.emptyMap());
            List<InspectDto> toImportInspects = inspectTasks.values().stream()
                    .filter(t -> changedNames.contains(t.getName()))
                    .collect(Collectors.toList());

            Set<String> stoppedInspectIds = new HashSet<>();
            for (InspectDto fileInspect : toImportInspects) {
                List<InspectDto> found = inspectService.findByName(fileInspect.getName());
                if (CollectionUtils.isNotEmpty(found)) {
                    InspectDto existing = found.get(0);
                    String existStatus = existing.getStatus();
                    if (InspectStatusEnum.RUNNING.getValue().equals(existStatus)
                            || InspectStatusEnum.SCHEDULING.getValue().equals(existStatus)) {
                        log.info("Stopping inspect task '{}' before import, status={}", existing.getName(), existStatus);
                        boolean stopped = stopAndWaitInspectTask(existing.getId().toHexString(), existing.getName(), user);
                        if (stopped) {
                            stoppedInspectIds.add(existing.getId().toHexString());
                        } else {
                            log.warn("Inspect task '{}' did not stop, skipping import", existing.getName());
                            toImportInspects.removeIf(t -> existing.getName().equals(t.getName()));
                        }
                    }
                }
            }

            if (!toImportInspects.isEmpty()) {
                refreshBaseDtoLastUpdate(toImportInspects);
                Map<String, String> taskIdToNameMap = buildTaskIdToNameMap(migrateTasks, syncTasks);
                // 打印 conMap 内容，key=导出时旧连接ID，value=导入后新连接ID/名称，用于排查 inspect 数据源找不到的问题
                if (log.isDebugEnabled()) {
                    conMap.forEach((oldId, dto) -> log.debug(
                            "conMap entry: oldId={}, newId={}, name={}",
                            oldId, dto != null ? dto.getId() : "null", dto != null ? dto.getName() : "null"));
                } else {
                    log.info("conMap size={}, keys={}", conMap.size(), conMap.keySet());
                }
                inspectService.importTaskByGroup(toImportInspects, taskIdMap, taskIdToNameMap, conMap, user);
                log.info("Inspect task import completed, count={}", toImportInspects.size());
            }

            // 重启导入前被停止的校验任务，使用 doExecuteInspect(SCHEDULING) 触发正式调度
            for (String stoppedId : stoppedInspectIds) {
                Where restartWhere = new Where();
                restartWhere.put("id", stoppedId);
                InspectDto scheduleDto = new InspectDto();
                scheduleDto.setStatus(InspectStatusEnum.SCHEDULING.getValue());
                inspectService.doExecuteInspect(restartWhere, scheduleDto, user);
            }

            log.info("Async import tasks standalone completed, recordId={}, tasks={}, inspects={}",
                    recordId, toImportTasks.size(), toImportInspects.size());
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, new ArrayList<>(), user);
        } catch (Exception e) {
            log.error("Async import tasks standalone failed, recordId={}", recordId, e);
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), new ArrayList<>(), user);
            throw new BizException("Group.Import.Failed", e.getMessage());
        }
    }

    /** 校验任务处于这些状态时视为已停止 */
    private static final Set<String> INSPECT_STOPPED_STATUSES = Set.of(
            InspectStatusEnum.DONE.getValue(),
            InspectStatusEnum.PASSED.getValue(),
            InspectStatusEnum.FAILED.getValue(),
            InspectStatusEnum.ERROR.getValue()
    );

    /**
     * 停止校验任务并等待其进入已停止状态，超时后返回 false。
     * 使用 doExecuteInspect(STOPPING) 触发正式停止流程（内部调 inspectTaskStop）。
     * 处理 InterruptedException：恢复中断标志后提前返回 false。
     */
    private boolean stopAndWaitInspectTask(String inspectId, String name, UserDetail user) {
        Where where = new Where();
        where.put("id", inspectId);
        InspectDto stopDto = new InspectDto();
        stopDto.setStatus(InspectStatusEnum.STOPPING.getValue());
        inspectService.doExecuteInspect(where, stopDto, user);

        long timeout = 60_000;
        long start = System.currentTimeMillis();
        while (true) {
            List<InspectDto> found = inspectService.findByName(name);
            if (CollectionUtils.isEmpty(found)) return true;
            String currentStatus = found.get(0).getStatus();
            if (INSPECT_STOPPED_STATUSES.contains(currentStatus)) {
                return true;
            }
            if (System.currentTimeMillis() - start > timeout) {
                log.error("Inspect task '{}' did not stop within {}ms, current status={}",
                        name, timeout, currentStatus);
                return false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for inspect task '{}' to stop", name);
                return false;
            }
        }
    }

    /**
     * 停止任务并等待其进入 stop 状态，超时后强制停止
     */
    private void stopAndWaitTask(TaskDto task, UserDetail user) {
        taskService.pause(task.getId(), user, false);
        long timeout = 60_000;
        long start = System.currentTimeMillis();
        TaskDto current = task;
        while (!TaskDto.STATUS_STOP.equals(current.getStatus())) {
            if (System.currentTimeMillis() - start > timeout) {
                log.warn("Task stop timeout, forcing stop. taskId={}", task.getId());
                taskService.pause(task.getId(), user, true);
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            current = taskService.findOne(new Query(Criteria.where("_id").is(task.getId())), user);
            if (current == null) break;
        }
    }

    // ====================== Private helpers ======================

    /** 从任务 DTO 集合中构建 oldTaskId → taskName 映射，用于校验任务 flowId 按名称解析 */
    @SuppressWarnings("unchecked")
    private Map<String, String> buildTaskIdToNameMap(Map<String, TaskDto>... taskMaps) {
        Map<String, String> result = new HashMap<>();
        for (Map<String, TaskDto> m : taskMaps) {
            m.values().forEach(t -> {
                if (t.getId() != null && StringUtils.isNotBlank(t.getName())) {
                    result.put(t.getId().toHexString(), t.getName());
                }
            });
        }
        return result;
    }

    private Map<String, List<TaskUpAndLoadDto>> parseImportPayloads(MultipartFile file) throws IOException {
        GroupTransferStrategy strategy = transferStrategyRegistry.getStrategy(GroupTransferType.FILE);
        return strategy.parseImportPayloads(file);
    }

    /** 连接比对重要字段（精确比对+记录 from/to）。config 全量比对不依赖此列表 */
    private static final List<String> CONNECTION_IMPORTANT_FIELDS = Arrays.asList(
            "connection_type",          // Source/Target/Source&Target
            "shareCdcEnable",           // 共享CDC开关
            "accessNodeType",           // 分配引擎模式
            "accessNodeProcessIdList",  // 分配引擎列表
            "heartbeatEnable",          // 是否开启心跳功能
            "schemaUpdateHour",         // 自动定时加载schema
            "openTableExcludeFilter",   // 是否开启表名过滤
            "table_filter",             // 包含表
            "tableExcludeFilter"        // 排除表
    );

    /** 顶层字段名到 getter 的反射式访问 */
    private static Object getTopLevelFieldValue(DataSourceConnectionDto conn, String field) {
        if (conn == null || field == null) return null;
        return switch (field) {
            case "connection_type" -> conn.getConnection_type();
            case "shareCdcEnable" -> conn.getShareCdcEnable();
            case "accessNodeType" -> conn.getAccessNodeType();
            case "accessNodeProcessIdList" -> conn.getAccessNodeProcessIdList();
            case "heartbeatEnable" -> conn.getHeartbeatEnable();
            case "schemaUpdateHour" -> conn.getSchemaUpdateHour();
            case "openTableExcludeFilter" -> conn.getOpenTableExcludeFilter();
            case "table_filter" -> conn.getTable_filter();
            case "tableExcludeFilter" -> conn.getTableExcludeFilter();
            default -> null;
        };
    }

    private ResourceDiff buildConnectionDiff(Map<String, List<TaskUpAndLoadDto>> payloads, UserDetail user) {
        ResourceDiff diff = new ResourceDiff();
        List<TaskUpAndLoadDto> connPayload = payloads.getOrDefault("Connection.json", Collections.emptyList());

        // Parse file connections (deduplicated by name)
        Map<String, DataSourceConnectionDto> fileConnsByName = new LinkedHashMap<>();
        for (TaskUpAndLoadDto item : connPayload) {
            if (!GroupConstants.COLLECTION_CONNECTION.equals(item.getCollectionName())) continue;
            DataSourceConnectionDto dto = parseConnectionDto(item.getJson());
            if (dto != null && StringUtils.isNotBlank(dto.getName())) {
                fileConnsByName.putIfAbsent(dto.getName(), dto);
            }
        }
        if (fileConnsByName.isEmpty()) return diff;

        // Batch-query existing connections by name
        Map<String, DataSourceConnectionDto> existingByName = dataSourceService
                .findAllDto(new Query(Criteria.where("name").in(fileConnsByName.keySet()).and("is_deleted").ne(true)), user)
                .stream().collect(Collectors.toMap(DataSourceConnectionDto::getName, c -> c, (a, b) -> a));

        // Batch-query definitions by pdkHash
        Set<String> pdkHashes = new HashSet<>();
        fileConnsByName.values().forEach(c -> { if (c.getPdkHash() != null) pdkHashes.add(c.getPdkHash()); });
        existingByName.values().forEach(c -> { if (c.getPdkHash() != null) pdkHashes.add(c.getPdkHash()); });
        Map<String, DataSourceDefinitionDto> defByPdkHash = pdkHashes.isEmpty()
                ? Collections.emptyMap()
                : dataSourceDefinitionService.findByPdkHashList(pdkHashes, user)
                    .stream().collect(Collectors.toMap(
                        DataSourceDefinitionDto::getPdkHash, d -> d, (a, b) -> a));

        // Inject vault secrets into file connections before comparison
        Map<String, String> vaultSecrets = parseVaultSecrets(payloads);
        if (MapUtils.isNotEmpty(vaultSecrets)) {
            for (DataSourceConnectionDto fileConn : fileConnsByName.values()) {
                try {
                    String pdkHash = fileConn.getPdkHash();
                    DataSourceDefinitionDto def = pdkHash != null ? defByPdkHash.get(pdkHash) : null;
                    ResourceHandler.injectVaultSecretsToConnection(fileConn, vaultSecrets, def);
                } catch (Exception e) {
                    log.warn("Vault inject failed for connection '{}' during diff, skipping: {}",
                            fileConn.getName(), e.getMessage());
                }
            }
        }

        for (Map.Entry<String, DataSourceConnectionDto> entry : fileConnsByName.entrySet()) {
            String name = entry.getKey();
            DataSourceConnectionDto fileConn = entry.getValue();
            DataSourceConnectionDto existingConn = existingByName.get(name);
            if (existingConn == null) {
                ResourceDiffItem item = new ResourceDiffItem(name, null);
                item.setDatabaseType(fileConn.getDatabase_type());
                item.setConnectionType(fileConn.getConnection_type());
                diff.getAdd().add(item);
            } else {
                // Resolve definition (prefer existing connection's pdkHash, fallback to file's)
                String pdkHash = existingConn.getPdkHash() != null ? existingConn.getPdkHash() : fileConn.getPdkHash();
                DataSourceDefinitionDto definition = pdkHash != null ? defByPdkHash.get(pdkHash) : null;

                List<FieldChange> changes = getConnectionChangedFields(fileConn, existingConn);
                if (!changes.isEmpty()) {
                    ResourceDiffItem item = new ResourceDiffItem(name, null, changes);
                    // Build fieldLabels from spec.json
                    if (definition != null) {
                        Map<String, String> configPathToLabel = ResourceHandler.buildConfigPathToLabelMap(definition);
                        Map<String, String> fieldLabels = new HashMap<>();
                        for (FieldChange change : changes) {
                            String fieldName = change.getField();
                            if (fieldName.startsWith("config.")) {
                                String configKey = fieldName.substring("config.".length());
                                String label = configPathToLabel.get(configKey);
                                if (label != null) fieldLabels.put(fieldName, label);
                            }
                        }
                        if (!fieldLabels.isEmpty()) item.setFieldLabels(fieldLabels);
                    }
                    diff.getUpdate().add(item);
                }
            }
        }
        return diff;
    }

    private List<FieldChange> getConnectionChangedFields(
            DataSourceConnectionDto fileConn,
            DataSourceConnectionDto existingConn) {
        List<FieldChange> changes = new ArrayList<>();

        // 1. 顶层字段比对
        for (String field : CONNECTION_IMPORTANT_FIELDS) {
            Object from = getTopLevelFieldValue(existingConn, field);
            Object to = getTopLevelFieldValue(fileConn, field);
            addFieldChange(changes, field, from, to);
        }

        // 2. Config 全字段精确比对
        Map<String, Object> fileConfig = normalizeConfigForComparison(fileConn.getConfig());
        Map<String, Object> existingConfig = normalizeConfigForComparison(existingConn.getConfig());
        Map<String, Object> existingConfigFiltered = filterToFileKeys(fileConfig, existingConfig);

        Set<String> allConfigKeys = new LinkedHashSet<>();
        if (fileConfig != null) allConfigKeys.addAll(fileConfig.keySet());
        if (existingConfigFiltered != null) allConfigKeys.addAll(existingConfigFiltered.keySet());

        for (String configPath : allConfigKeys) {
            Object from = existingConfigFiltered != null ? existingConfigFiltered.get(configPath) : null;
            Object to = fileConfig != null ? fileConfig.get(configPath) : null;
            if (jsonEqual(from, to)) continue;

            if (MASKED_DISPLAY_API_KEYS.contains(configPath)) {
                changes.add(new FieldChange("config." + configPath, "******", "******"));
            } else if (URI_DISPLAY_API_KEYS.contains(configPath)) {
                changes.add(new FieldChange("config." + configPath, maskUriValue(from), maskUriValue(to)));
            } else {
                changes.add(new FieldChange("config." + configPath, from, to));
            }
        }

        return changes;
    }


    private void addFieldChange(List<FieldChange> changes, String field, Object from, Object to) {
        if (!Objects.equals(from, to)) changes.add(new FieldChange(field, from, to));
    }

    /** 需要完全 mask 展示值的 apiServerKey（密码类字段） */
    private static final Set<String> MASKED_DISPLAY_API_KEYS = Set.of(
            "database_password"
    );

    /** 需要智能 mask 的 URI 类 apiServerKey：MongoDB URI 只 mask 密码部分，其他 URI 全部 mask */
    private static final Set<String> URI_DISPLAY_API_KEYS = Set.of(
            "database_uri"
    );

    /**
     * 对 URI 值进行智能 mask：
     * <ul>
     *   <li>尝试用 MongoDB {@link ConnectionString} 解析，成功则只将密码替换为 ******</li>
     *   <li>解析失败（非 MongoDB URI）则整个值替换为 ******</li>
     *   <li>null 值返回 null</li>
     * </ul>
     */
    static Object maskUriValue(Object value) {
        if (value == null) return null;
        String uri = String.valueOf(value);
        try {
            ConnectionString cs = new ConnectionString(uri);
            char[] pwd = cs.getPassword();
            if (pwd != null && pwd.length > 0) {
                String username = cs.getUsername();
                // 在原始 URI 中定位 username: 和 @ 之间的部分（即 URL 编码的密码），替换为 ******
                String prefix = username + ":";
                int credStart = uri.indexOf("//") + 2;
                int pwdStart = uri.indexOf(prefix, credStart) + prefix.length();
                int pwdEnd = uri.indexOf("@", pwdStart);
                if (pwdStart > prefix.length() && pwdEnd > pwdStart) {
                    return uri.substring(0, pwdStart) + "******" + uri.substring(pwdEnd);
                }
            }
            // MongoDB URI 但没有密码，原样返回
            return uri;
        } catch (Exception e) {
            // 非 MongoDB URI，全部 mask
            return "******";
        }
    }

    /** 移除 config 中环境相关字段，返回用于对比的副本。敏感字段保留以参与比对。 */
    private Map<String, Object> normalizeConfigForComparison(Map<String, Object> config) {
        if (config == null) return null;
        Map<String, Object> copy = new HashMap<>(config);
        CONFIG_ENV_EXCLUDED_FIELDS.forEach(copy::remove);
        return copy;
    }

    /**
     * 从 existingConfig 中只保留 fileConfig 中存在的 key。
     * 文件中没有的字段说明导出时被剔除（如 uri、加密密码等），导入时不会覆盖，不应参与 diff 比较。
     */
    private Map<String, Object> filterToFileKeys(Map<String, Object> fileConfig, Map<String, Object> existingConfig) {
        if (fileConfig == null || existingConfig == null) return existingConfig;
        Map<String, Object> filtered = new HashMap<>();
        for (String key : fileConfig.keySet()) {
            if (existingConfig.containsKey(key)) {
                filtered.put(key, existingConfig.get(key));
            }
        }
        return filtered;
    }

    private DataSourceConnectionDto parseConnectionDto(String json) {
        if (StringUtils.isBlank(json)) return null;
        try {
            return JsonUtil.parseJsonUseJackson(json, DataSourceConnectionDto.class);
        } catch (Exception e) {
            log.warn("Failed to parse DataSourceConnectionDto from JSON for diff comparison", e);
            return null;
        }
    }

    private ResourceDiff buildTaskDiff(Map<String, List<TaskUpAndLoadDto>> payloads, UserDetail user) {
        ResourceDiff diff = new ResourceDiff();

        // Collect parsed TaskDto entries with their display type
        List<Map.Entry<TaskDto, String>> fileTaskEntries = new ArrayList<>();
        for (TaskUpAndLoadDto item : payloads.getOrDefault("MigrateTask.json", Collections.emptyList())) {
            if (GroupConstants.COLLECTION_TASK.equals(item.getCollectionName())) {
                TaskDto dto = parseTaskDto(item.getJson());
                if (dto != null && StringUtils.isNotBlank(dto.getName())) {
                    fileTaskEntries.add(Map.entry(dto, "migrate"));
                }
            }
        }
        for (TaskUpAndLoadDto item : payloads.getOrDefault("SyncTask.json", Collections.emptyList())) {
            if (GroupConstants.COLLECTION_TASK.equals(item.getCollectionName())) {
                TaskDto dto = parseTaskDto(item.getJson());
                if (dto != null && StringUtils.isNotBlank(dto.getName())) {
                    fileTaskEntries.add(Map.entry(dto, "sync"));
                }
            }
        }

        // Regular tasks: query DB then config-compare (mirrors checkTaskConfig logic)
        if (!fileTaskEntries.isEmpty()) {
            List<String> regularTaskNames = fileTaskEntries.stream()
                    .map(e -> e.getKey().getName()).collect(Collectors.toList());
            Map<String, TaskDto> existingByName = taskService
                    .findAllDto(new Query(Criteria.where("name").in(regularTaskNames).and("is_deleted").ne(true)), user)
                    .stream().collect(Collectors.toMap(TaskDto::getName, t -> t, (a, b) -> a));
            for (Map.Entry<TaskDto, String> entry : fileTaskEntries) {
                TaskDto fileTask = entry.getKey();
                String type = entry.getValue();
                TaskDto existingTask = existingByName.get(fileTask.getName());
                if (existingTask == null) {
                    ResourceDiffItem item = new ResourceDiffItem(fileTask.getName(), type);
                    item.setSyncType(fileTask.getType());
                    item.setTableMapping(extractTableMapping(fileTask));
                    diff.getAdd().add(item);
                } else {
                    // normalize ownerId (same as checkTaskConfig) before comparing
                    if (fileTask.getDag() != null) {
                        fileTask.getDag().setOwnerId(null);
                    }
                    DagChangeDetail dagChangeDetail = new DagChangeDetail();
                    List<FieldChange> changes = TaskConfigCompareUtil.getDetailedChanges(fileTask, existingTask, dagChangeDetail);
                    if (!changes.isEmpty() || dagChangeDetail.hasChanges()) {
                        ResourceDiffItem diffItem = new ResourceDiffItem(fileTask.getName(), type, changes);
                        if (dagChangeDetail.hasChanges()) {
                            diffItem.setDagChangeDetail(dagChangeDetail);
                        }
                        diff.getUpdate().add(diffItem);
                    }
                    // config equal → no change, skip
                }
            }
        }

        // Inspect/validate tasks: parse full DTO and do field-level comparison
        for (TaskUpAndLoadDto item : payloads.getOrDefault("InspectTask.json", Collections.emptyList())) {
            if (!GroupConstants.COLLECTION_INSPECT.equals(item.getCollectionName())) continue;
            InspectDto fileInspect = parseInspectDto(item.getJson());
            if (fileInspect == null || StringUtils.isBlank(fileInspect.getName())) continue;
            List<InspectDto> found = inspectService.findByName(fileInspect.getName());
            if (CollectionUtils.isEmpty(found)) {
                diff.getAdd().add(new ResourceDiffItem(fileInspect.getName(), "validate"));
            } else {
                List<FieldChange> changes = getInspectChangedFields(fileInspect, found.get(0));
                if (!changes.isEmpty()) {
                    diff.getUpdate().add(new ResourceDiffItem(fileInspect.getName(), "validate", changes));
                }
                // config equal → no change, skip
            }
        }

        return diff;
    }

    private LinkedHashMap<String, String> extractTableMapping(TaskDto task) {
        LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
        DAG dag = task.getDag();
        if (dag == null) return mapping;

        String syncType = task.getSyncType();
        if ("migrate".equals(syncType)) {
            List<Node> targets = dag.getTargets();
            if (targets != null) {
                for (Node target : targets) {
                    if (target instanceof DatabaseNode) {
                        DatabaseNode dbNode = (DatabaseNode) target;
                        List<SyncObjects> syncObjects = dbNode.getSyncObjects();
                        if (syncObjects != null) {
                            for (SyncObjects so : syncObjects) {
                                if ("table".equals(so.getType())) {
                                    LinkedHashMap<String, String> relation = so.getTableNameRelation();
                                    if (relation != null) {
                                        mapping.putAll(relation);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else if ("sync".equals(syncType)) {
            List<Node> targets = dag.getTargets();
            if (targets != null) {
                for (Node target : targets) {
                    if (target instanceof TableNode) {
                        String targetTableName = ((TableNode) target).getTableName();
                        List<TableNode> sourceNodes = dag.getSourceTableNodes(target.getId());
                        if (sourceNodes != null && !sourceNodes.isEmpty()) {
                            for (TableNode src : sourceNodes) {
                                mapping.put(src.getTableName(), targetTableName);
                            }
                        } else {
                            List<Node> sources = dag.getSources();
                            if (sources != null) {
                                for (Node source : sources) {
                                    if (source instanceof TableNode) {
                                        mapping.put(((TableNode) source).getTableName(), targetTableName);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return mapping;
    }

    private Map<String, List<TaskUpAndLoadDto>> filterPayloadsByTaskType(
            Map<String, List<TaskUpAndLoadDto>> payloads, String taskTypeKey) {
        Map<String, List<TaskUpAndLoadDto>> filtered = new HashMap<>(payloads);

        // 1. Build oldTaskId → taskName from all task payloads
        Map<String, String> allTaskIdToName = new HashMap<>();
        for (String key : List.of("MigrateTask.json", "SyncTask.json")) {
            for (TaskUpAndLoadDto item : payloads.getOrDefault(key, Collections.emptyList())) {
                if (GroupConstants.COLLECTION_TASK.equals(item.getCollectionName())) {
                    TaskDto dto = parseTaskDto(item.getJson());
                    if (dto != null && dto.getId() != null && StringUtils.isNotBlank(dto.getName())) {
                        allTaskIdToName.put(dto.getId().toHexString(), dto.getName());
                    }
                }
            }
        }

        // 2. Collect target type task names
        Set<String> targetTaskNames = new HashSet<>();
        for (TaskUpAndLoadDto item : payloads.getOrDefault(taskTypeKey, Collections.emptyList())) {
            if (GroupConstants.COLLECTION_TASK.equals(item.getCollectionName())) {
                TaskDto dto = parseTaskDto(item.getJson());
                if (dto != null && StringUtils.isNotBlank(dto.getName())) {
                    targetTaskNames.add(dto.getName());
                }
            }
        }

        // 3. Remove the other task type
        if ("MigrateTask.json".equals(taskTypeKey)) {
            filtered.remove("SyncTask.json");
        } else {
            filtered.remove("MigrateTask.json");
        }

        // 4. Filter InspectTask: keep only inspects whose flowId maps to a target type task
        List<TaskUpAndLoadDto> allInspects = filtered.getOrDefault("InspectTask.json", Collections.emptyList());
        if (!allInspects.isEmpty()) {
            List<TaskUpAndLoadDto> filteredInspects = allInspects.stream()
                    .filter(item -> {
                        if (!GroupConstants.COLLECTION_INSPECT.equals(item.getCollectionName())) return false;
                        InspectDto inspect = parseInspectDto(item.getJson());
                        if (inspect == null || StringUtils.isBlank(inspect.getFlowId())) return false;
                        String taskName = allTaskIdToName.get(inspect.getFlowId());
                        return taskName != null && targetTaskNames.contains(taskName);
                    })
                    .collect(Collectors.toList());
            filtered.put("InspectTask.json", filteredInspects);
        }

        return filtered;
    }

    private TaskDto parseTaskDto(String json) {
        if (StringUtils.isBlank(json)) return null;
        try {
            return JsonUtil.parseJsonUseJackson(json, TaskDto.class);
        } catch (Exception e) {
            log.warn("Failed to parse TaskDto from JSON for diff comparison", e);
            return null;
        }
    }

    private InspectDto parseInspectDto(String json) {
        if (StringUtils.isBlank(json)) return null;
        try {
            return JsonUtil.parseJsonUseJackson(json, InspectDto.class);
        } catch (Exception e) {
            log.warn("Failed to parse InspectDto from JSON for diff comparison", e);
            return null;
        }
    }

    private List<FieldChange> getInspectChangedFields(InspectDto fileInspect, InspectDto existingInspect) {
        List<FieldChange> changes = new ArrayList<>();
        // from = existingInspect (DB), to = fileInspect (import)
        addFieldChange(changes, "flowId", existingInspect.getFlowId(), fileInspect.getFlowId());
        addFieldChange(changes, "mode", existingInspect.getMode(), fileInspect.getMode());
        addFieldChange(changes, "inspectMethod", existingInspect.getInspectMethod(), fileInspect.getInspectMethod());
        addFieldChange(changes, "inspectDifferenceMode", existingInspect.getInspectDifferenceMode(), fileInspect.getInspectDifferenceMode());
        addFieldChangeJson(changes, "timing", existingInspect.getTiming(), fileInspect.getTiming());
        addFieldChangeJson(changes, "limit", existingInspect.getLimit(), fileInspect.getLimit());
        addFieldChange(changes, "enabled", existingInspect.getEnabled(), fileInspect.getEnabled());
        addFieldChange(changes, "roundingMode", existingInspect.getRoundingMode(), fileInspect.getRoundingMode());
        // byFirstCheckId 是跨环境迁移后无意义的关联字段，不参与对比
        addFieldChange(changes, "browserTimezoneOffset", existingInspect.getBrowserTimezoneOffset(), fileInspect.getBrowserTimezoneOffset());
        addFieldChange(changes, "cdcDuration", existingInspect.getCdcDuration(), fileInspect.getCdcDuration());
        addFieldChange(changes, "checkTableThreadNum", existingInspect.getCheckTableThreadNum(), fileInspect.getCheckTableThreadNum());
        addFieldChangeJson(changes, "alarmSettings", existingInspect.getAlarmSettings(), fileInspect.getAlarmSettings());
        if (!inspectTasksEqual(fileInspect.getTasks(), existingInspect.getTasks())) {
            changes.add(new FieldChange("tasks", existingInspect.getTasks(), fileInspect.getTasks()));
        }
        return changes;
    }

    private void addFieldChangeJson(List<FieldChange> changes, String field, Object from, Object to) {
        if (from == null && to == null) return;
        if (from == null || to == null) { changes.add(new FieldChange(field, from, to)); return; }
        try {
            if (!Objects.equals(JsonUtil.toJsonUseJackson(from), JsonUtil.toJsonUseJackson(to))) {
                changes.add(new FieldChange(field, from, to));
            }
        } catch (Exception e) {
            if (!Objects.equals(from, to)) changes.add(new FieldChange(field, from, to));
        }
    }

    private boolean inspectTasksEqual(List<com.tapdata.tm.inspect.bean.Task> fileTasks,
                                      List<com.tapdata.tm.inspect.bean.Task> existingTasks) {
        if (fileTasks == null && existingTasks == null) return true;
        if (fileTasks == null || existingTasks == null) return false;
        if (fileTasks.size() != existingTasks.size()) return false;
        List<Map<String, Object>> normFile = fileTasks.stream()
                .map(this::normalizeInspectTask)
                .sorted(Comparator.comparing(this::buildInspectTaskKey))
                .collect(Collectors.toList());
        List<Map<String, Object>> normExisting = existingTasks.stream()
                .map(this::normalizeInspectTask)
                .sorted(Comparator.comparing(this::buildInspectTaskKey))
                .collect(Collectors.toList());
        try {
            return Objects.equals(JsonUtil.toJsonUseJackson(normFile), JsonUtil.toJsonUseJackson(normExisting));
        } catch (Exception e) {
            log.warn("Failed to compare inspect task lists", e);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeInspectTask(com.tapdata.tm.inspect.bean.Task task) {
        try {
            Map<String, Object> map = JsonUtil.parseJsonUseJackson(
                    JsonUtil.toJsonUseJackson(task), new TypeReference<Map<String, Object>>() {});
            if (map == null) return Collections.emptyMap();
            map.remove("taskId");
            for (String side : new String[]{"source", "target"}) {
                Object sideObj = map.get(side);
                if (sideObj instanceof Map) {
                    ((Map<String, Object>) sideObj).remove("connectionId");
                    ((Map<String, Object>) sideObj).remove("nodeId");
                }
            }
            return map;
        } catch (Exception e) {
            log.warn("Failed to normalize inspect task item", e);
            return Collections.emptyMap();
        }
    }

    @SuppressWarnings("unchecked")
    private String buildInspectTaskKey(Map<String, Object> normalizedTask) {
        // uniqueness: source(connectionName+nodeName+databaseType+table) + target(same)
        StringBuilder sb = new StringBuilder();
        for (String side : new String[]{"source", "target"}) {
            Object sideObj = normalizedTask.get(side);
            if (sideObj instanceof Map) {
                Map<String, Object> m = (Map<String, Object>) sideObj;
                sb.append(m.getOrDefault("connectionName", "")).append("|")
                  .append(m.getOrDefault("nodeName", "")).append("|")
                  .append(m.getOrDefault("databaseType", "")).append("|")
                  .append(m.getOrDefault("table", "")).append("||");
            }
        }
        return sb.toString();
    }

    /** API 对比时排除的顶层字段：环境相关、运行时状态字段 */
    private static final Set<String> MODULE_EXCLUDED_FIELDS = new HashSet<>(Arrays.asList(
            "id", "connectionId", "connection", "createTime", "datasource",
            "createUser", "lastUpdBy", "status", "isDeleted"
    ));

    private static final Set<String> CONFIG_ENV_EXCLUDED_FIELDS = Collections.emptySet();

    private ResourceDiff buildApiDiff(Map<String, List<TaskUpAndLoadDto>> payloads, UserDetail user) {
        ResourceDiff diff = new ResourceDiff();

        // 解析文件中的 Module，以 name 为 key 去重
        Map<String, Map<String, Object>> fileModulesByName = new LinkedHashMap<>();
        for (TaskUpAndLoadDto item : payloads.getOrDefault("Module.json", Collections.emptyList())) {
            if (!GroupConstants.COLLECTION_MODULES.equals(item.getCollectionName())) continue;
            Map<String, Object> normalized = normalizeModuleForComparison(item.getJson());
            if (normalized == null) continue;
            String name = (String) normalized.get("name");
            if (StringUtils.isNotBlank(name)) fileModulesByName.putIfAbsent(name, normalized);
        }
        if (fileModulesByName.isEmpty()) return diff;

        // 批量查询 DB 中同名的 Module
        Map<String, ModulesDto> existingByName = modulesService
                .findAllDto(new Query(Criteria.where("name").in(fileModulesByName.keySet()).and("is_deleted").ne(true)), user)
                .stream().collect(Collectors.toMap(ModulesDto::getName, m -> m, (a, b) -> a));

        for (Map.Entry<String, Map<String, Object>> entry : fileModulesByName.entrySet()) {
            String name = entry.getKey();
            Map<String, Object> fileNormalized = entry.getValue();
            ModulesDto existingModule = existingByName.get(name);
            if (existingModule == null) {
                ResourceDiffItem item = new ResourceDiffItem(name, null);
                item.setApiPath((String) fileNormalized.get("path"));
                item.setApiConnectionName((String) fileNormalized.get("connectionName"));
                item.setApiTableName((String) fileNormalized.get("tableName"));
                diff.getAdd().add(item);
            } else {
                Map<String, Object> existingNormalized = normalizeModuleForComparison(JsonUtil.toJsonUseJackson(existingModule));
                List<FieldChange> changedFields = getModuleChangedFields(fileNormalized, existingNormalized);
                if (!changedFields.isEmpty()) {
                    diff.getUpdate().add(new ResourceDiffItem(name, null, changedFields));
                }
                // 内容一致 → 跳过
            }
        }
        return diff;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeModuleForComparison(String json) {
        if (StringUtils.isBlank(json)) return null;
        try {
            Map<String, Object> map = JsonUtil.parseJsonUseJackson(json, new TypeReference<Map<String, Object>>() {});
            if (map == null) return null;
            // 移除顶层排除字段
            MODULE_EXCLUDED_FIELDS.forEach(map::remove);
            // 移除 fields 数组中每个元素的 id
            Object fieldsObj = map.get("fields");
            if (fieldsObj instanceof List) {
                for (Object fieldItem : (List<?>) fieldsObj) {
                    if (fieldItem instanceof Map) ((Map<String, Object>) fieldItem).remove("id");
                }
            }
            // 移除 listtags 数组中每个元素的 id
            Object listtagsObj = map.get("listtags");
            if (listtagsObj instanceof List) {
                for (Object tagItem : (List<?>) listtagsObj) {
                    if (tagItem instanceof Map) ((Map<String, Object>) tagItem).remove("id");
                }
            }
            return map;
        } catch (Exception e) {
            log.warn("Failed to normalize ModulesDto for comparison", e);
            return null;
        }
    }

    private List<FieldChange> getModuleChangedFields(Map<String, Object> fileMap, Map<String, Object> existingMap) {
        if (fileMap == null && existingMap == null) return Collections.emptyList();
        if (fileMap == null || existingMap == null) return Collections.singletonList(new FieldChange("*", existingMap, fileMap));
        List<FieldChange> changes = new ArrayList<>();
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(fileMap.keySet());
        allKeys.addAll(existingMap.keySet());
        for (String key : allKeys) {
            deepDiff(changes, key, existingMap.get(key), fileMap.get(key));
        }
        return changes;
    }

    /**
     * 通用递归 diff，三段式逻辑：
     *   ① JSON 归一化比较 → 相等直接 return（early-exit，避免不必要的递归）
     *   ② 收集这一层所有 key 及两侧的 value，合并成 pairs
     *   ③ 对每个 pair 递归调用自身
     *
     * 支持类型：
     *   - Map  → 以 key 为路径递归，路径格式 "parent.child"
     *   - List → 按路径配置表查找 keyField 做 keyed diff，
     *            配置表中不存在的路径则按下标，路径格式 "parent[keyVal]" 或 "parent[0]"
     *   - 叶子 → 直接记录 FieldChange
     *
     * 数组 key 配置表：路径中 [*] 匹配任意 keyed 段，显式声明各数组使用哪个字段作标识符。
     * 注意：_id 不可用作 key，跨环境导入后两侧 _id 不同。
     */
    private static final Map<String, String> ARRAY_KEY_CONFIG = Map.of(
            "fields",                        "field_name",
            "paths",                         "name",
            "paths[*].fields",               "field_name",
            "paths[*].availableQueryField",  "field_name",
            "paths[*].requiredQueryField",   "field_name"
    );

    @SuppressWarnings("unchecked")
    private void deepDiff(List<FieldChange> changes, String path, Object existing, Object file) {
        // ① JSON 归一化比较：相等则剪枝，不再往下递归
        if (jsonEqual(existing, file)) return;

        if (existing instanceof Map && file instanceof Map) {
            Map<String, Object> existingMap = (Map<String, Object>) existing;
            Map<String, Object> fileMap     = (Map<String, Object>) file;

            // ② 收集这一层所有 key → (existing值, file值) 的 pairs
            Map<String, Object[]> pairs = new LinkedHashMap<>();
            existingMap.forEach((k, v) -> pairs.computeIfAbsent(k, x -> new Object[2])[0] = v);
            fileMap.forEach((k, v)     -> pairs.computeIfAbsent(k, x -> new Object[2])[1] = v);

            // ③ 对每个 pair 递归
            pairs.forEach((k, pair) -> deepDiff(changes, path + "." + k, pair[0], pair[1]));

        } else if (existing instanceof List && file instanceof List) {
            List<Map<String, Object>> existingList = toMapList(existing);
            List<Map<String, Object>> fileList     = toMapList(file);
            String keyField = ARRAY_KEY_CONFIG.get(normalizePathForConfig(path));

            if (keyField != null) {
                // ② 收集 keyed pairs：以配置表指定的字段为标识符
                Map<String, Object[]> pairs = new LinkedHashMap<>();
                existingList.forEach(item -> {
                    Object k = item.get(keyField);
                    if (k != null) pairs.computeIfAbsent(k.toString(), x -> new Object[2])[0] = item;
                });
                fileList.forEach(item -> {
                    Object k = item.get(keyField);
                    if (k != null) pairs.computeIfAbsent(k.toString(), x -> new Object[2])[1] = item;
                });
                // ③ 对每个 pair 递归
                pairs.forEach((k, pair) -> deepDiff(changes, path + "[" + k + "]", pair[0], pair[1]));
            } else {
                // ② 按下标收集 pairs（该路径不在配置表中）
                List<?> existingL = (List<?>) existing;
                List<?> fileL     = (List<?>) file;
                int maxLen = Math.max(existingL.size(), fileL.size());
                // ③ 对每个下标递归
                for (int i = 0; i < maxLen; i++) {
                    deepDiff(changes, path + "[" + i + "]",
                            i < existingL.size() ? existingL.get(i) : null,
                            i < fileL.size()     ? fileL.get(i)     : null);
                }
            }
        } else {
            // 叶子节点（或两侧类型不同）：经过 ① 已确认不相等，直接记录
            changes.add(new FieldChange(path, existing, file));
        }
    }

    /**
     * 将实际运行路径（如 "paths.[customerQuery].fields"）归一化为配置表的 key
     * （如 "paths.[*].fields"），方法是把 "[具体值]" 替换为 "[*]"。
     */
    private String normalizePathForConfig(String path) {
        return path.replaceAll("\\[([^*\\]]+)]", "[*]");
    }

    /**
     * 用 COMPARISON_MAPPER 对两个值做 JSON 归一化序列化后比较。
     * COMPARISON_MAPPER 会忽略 null 字段、对 key 排序，消除序列化顺序差异导致的误判。
     * 序列化失败时返回 false（保守策略：继续往下对比，不漏报差异）。
     * 空字符串与 null 视为相等（导出时字符串字段可能为 "" 而 DB 侧为 null）。
     */
    private boolean jsonEqual(Object a, Object b) {
        Object normA = normalizeEmptyString(a);
        Object normB = normalizeEmptyString(b);
        if (normA == null && normB == null) return true;
        try {
            return Objects.equals(
                    COMPARISON_MAPPER.writeValueAsString(normA),
                    COMPARISON_MAPPER.writeValueAsString(normB));
        } catch (Exception e) {
            log.warn("deepDiff: JSON serialization failed for path comparison, falling back to structural diff", e);
            return false;
        }
    }

    /** 将空字符串归一化为 null，使空字符串与 null 在对比时视为相等 */
    private static Object normalizeEmptyString(Object v) {
        return (v instanceof String && ((String) v).isEmpty()) ? null : v;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> toMapList(Object obj) {
        if (!(obj instanceof List)) return Collections.emptyList();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : (List<?>) obj) {
            if (item instanceof Map) result.add((Map<String, Object>) item);
        }
        return result;
    }

    private String extractNameFromJson(String json) {
        if (StringUtils.isBlank(json)) return null;
        try {
            Map<String, Object> map = JsonUtil.parseJsonUseJackson(json, new TypeReference<>() {});
            if (map != null && map.get("name") instanceof String name) {
                return name;
            }
        } catch (Exception ignored) {}
        return null;
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
                handler.collectPayloadRelatedResources(payloads, resourceMapsByType, metadataByType,user);
            }
        }

        List<GroupInfoDto> groupInfos = new ArrayList<>();
        collectGroupInfoPayload(groupPayload, groupInfos);
        Map<String, DataSourceConnectionDto> connections = (Map<String, DataSourceConnectionDto>) resourceMapsByType
                .getOrDefault(ResourceType.CONNECTION, Collections.emptyMap());
        try{
            batchUpChecker.checkDataSourceConnection(connections.values().stream().toList(), user,false);
        } catch (Exception e) {
            updateRecordStatus(recordId, GroupInfoRecordDto.STATUS_FAILED, ExceptionUtils.getMessage(e), null, user);
            throw new RuntimeException(e);
        }
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
            Map<String, String> tagMap = new HashMap<>();
            Map<String,MetadataDefinitionDto> metadataDefinitionDtoMap = (Map<String,MetadataDefinitionDto>) resourceMapsByType
                    .getOrDefault(ResourceType.METADATA_DEFINITION,Collections.emptyMap());
            tagMap = metadataDefinitionService.batchImport(new ArrayList<>(metadataDefinitionDtoMap.values()), user);
            checkTags(resourceMapsByType,tagMap);
            log.info("Stage 1: Start importing connection resources");
            // Inject vault secrets before importing connections
            Map<String, String> vaultSecrets = parseVaultSecrets(payloads);
            if (!vaultSecrets.isEmpty()) {
                log.info("Vault secrets found, injecting into {} connections", connections.size());
                injectVaultSecrets(connections, vaultSecrets, user);
            }
            List<MetadataInstancesDto> connectionMetadata = metadataByType
                    .getOrDefault(ResourceType.CONNECTION, Collections.emptyList());
            refreshImportLastUpdate(connections.values(), connectionMetadata);
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
                refreshBaseDtoLastUpdate(allTasks);
                taskImportResult = taskService.batchImport(allTasks, user, importMode, new ArrayList<>(), conMap, taskIdMap,
                        nodeIdMap, resetTaskList);
                List<MetadataInstancesDto> allTaskMetadata = new ArrayList<>();
                allTaskMetadata
                        .addAll(metadataByType.getOrDefault(ResourceType.MIGRATE_TASK, Collections.emptyList()));
                allTaskMetadata
                        .addAll(metadataByType.getOrDefault(ResourceType.SYNC_TASK, Collections.emptyList()));
                allTaskMetadata
                        .addAll(metadataByType.getOrDefault(ResourceType.SHARE_CACHE, Collections.emptyList()));
                refreshMetadataLastUpdate(allTaskMetadata);
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
                refreshBaseDtoLastUpdate(inspectTasks.values());
                Map<String, String> taskIdToNameMap = buildTaskIdToNameMap(migrateTasks, syncTasks);
                inspectService.importTaskByGroup(new ArrayList<>(inspectTasks.values()), taskIdMap, taskIdToNameMap, conMap, user);
                log.info("Inspect tasks import completed, inspectTaskCount={}", inspectTasks.size());
            }
            importedResources += inspectCount;
            updateImportProgress(recordId, calculateProgress(importedResources, totalResources), details, user);

            // Stage 4: Import modules
            Map<String, ModulesDto> modules = (Map<String, ModulesDto>) resourceMapsByType
                    .getOrDefault(ResourceType.MODULE, Collections.emptyMap());
            if (MapUtils.isNotEmpty(modules)) {
                refreshModuleLastUpdate(modules.values());
                moduleImportResult = modulesService.batchImport(new ArrayList<>(modules.values()), user, importMode, conMap, null);
                refreshMetadataLastUpdate(metadataByType.getOrDefault(ResourceType.MODULE, Collections.emptyList()));
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

    /**
     * 解析独立上传的 vault MultipartFile，返回 secret_name -> secret_value 映射。
     */
    private Map<String, String> parseVaultFile(MultipartFile vaultFile) throws IOException {
        if (vaultFile == null || vaultFile.isEmpty()) {
            return Collections.emptyMap();
        }
        String json = new String(vaultFile.getBytes(), StandardCharsets.UTF_8);
        if (StringUtils.isBlank(json)) {
            return Collections.emptyMap();
        }
        Map<String, String> secrets = JsonUtil.parseJsonUseJackson(json, new TypeReference<Map<String, String>>() {});
        return secrets != null ? secrets : Collections.emptyMap();
    }

    private void refreshImportLastUpdate(Collection<DataSourceConnectionDto> connections,
            Collection<MetadataInstancesDto> metadataInstances) {
        long currentTime = System.currentTimeMillis();
        if (CollectionUtils.isNotEmpty(connections)) {
            connections.stream().filter(Objects::nonNull).forEach(connection -> connection.setLastUpdate(currentTime));
        }
        refreshMetadataLastUpdate(metadataInstances, currentTime);
    }

    private void refreshMetadataLastUpdate(Collection<MetadataInstancesDto> metadataInstances) {
        refreshMetadataLastUpdate(metadataInstances, System.currentTimeMillis());
    }

    private void refreshMetadataLastUpdate(Collection<MetadataInstancesDto> metadataInstances, long currentTime) {
        if (CollectionUtils.isEmpty(metadataInstances)) {
            return;
        }
        metadataInstances.stream().filter(Objects::nonNull).forEach(metadata -> metadata.setLastUpdate(currentTime));
    }

    private void refreshBaseDtoLastUpdate(Collection<? extends BaseDto> dtos) {
        refreshBaseDtoLastUpdate(dtos, System.currentTimeMillis());
    }

    private void refreshBaseDtoLastUpdate(Collection<? extends BaseDto> dtos, long currentTime) {
        if (CollectionUtils.isEmpty(dtos)) {
            return;
        }
        dtos.stream().filter(Objects::nonNull).forEach(dto -> dto.setLastUpdAt(new Date(currentTime)));
    }

    private void refreshModuleLastUpdate(Collection<ModulesDto> modules) {
        long currentTime = System.currentTimeMillis();
        refreshBaseDtoLastUpdate(modules, currentTime);
        if (CollectionUtils.isEmpty(modules)) {
            return;
        }
        modules.stream().filter(Objects::nonNull).forEach(module -> module.setLast_updated(new Date(currentTime)));
    }

    /**
     * 从 payloads 中解析 vault.json，返回 secret_name -> secret_value 映射。
     * vault key 格式：{connectionName}_{fieldSuffix}，e.g. LIS_QMH_pg_HKPMI_host
     */
    private Map<String, String> parseVaultSecrets(Map<String, List<TaskUpAndLoadDto>> payloads) {
        List<TaskUpAndLoadDto> vaultPayload = payloads.getOrDefault(GroupConstants.VAULT_FILE, Collections.emptyList());
        if (CollectionUtils.isEmpty(vaultPayload)) {
            return Collections.emptyMap();
        }
        String json = vaultPayload.get(0).getJson();
        if (StringUtils.isBlank(json)) {
            return Collections.emptyMap();
        }
        Map<String, String> secrets = JsonUtil.parseJsonUseJackson(json, new TypeReference<Map<String, String>>() {});
        return secrets != null ? secrets : Collections.emptyMap();
    }

    /**
     * 将 vault 敏感信息注入连接 config，替换导出时已脱敏的字段。
     */
    private void injectVaultSecrets(Map<String, DataSourceConnectionDto> connections,
            Map<String, String> vaultSecrets, UserDetail user) {
        connections.values().forEach(conn -> {
            DataSourceDefinitionDto definition =
                    dataSourceDefinitionService.findByPdkHash(conn.getPdkHash(), Integer.MAX_VALUE, user);
            if (definition == null) {
                log.warn("Vault inject: definition not found for connection '{}', pdkHash={}, skipping schema BFS",
                        conn.getName(), conn.getPdkHash());
            }
            ResourceHandler.injectVaultSecretsToConnection(conn, vaultSecrets, definition);
        });
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

    /**
     * 查询连接关联的模型元数据（database 级别 + table/collection 等级别），序列化为导出 payload。
     * taskId 为空的条目为连接原始模型，区别于任务节点派生的模型（后者由 TaskResourceHandler 导出）。
     */
    private List<TaskUpAndLoadDto> buildConnectionMetadataPayload(Collection<String> connectionIds, UserDetail user) {
        List<TaskUpAndLoadDto> payload = new ArrayList<>();
        for (String connectionId : connectionIds) {
            try {
                // database 级别已由 buildConnectionPayload 导出，这里只补充 table/collection/view 等具体表结构
                // taskId 为空：连接原始模型；排除任务派生的模型（任务节点模型已由 TaskResourceHandler 导出）
                Query tableQuery = new Query(
                        Criteria.where("source._id").is(connectionId)
                                .and("metaType").ne("database")
                                .and("taskId").exists(false)
                                .and("is_deleted").ne(true));
                List<MetadataInstancesDto> tableMeta = metadataInstancesService.findAllDto(tableQuery, user);
                for (MetadataInstancesDto meta : tableMeta) {
                    meta.setCreateUser(null);
                    meta.setCustomId(null);
                    meta.setLastUpdBy(null);
                    meta.setUserId(null);
                    payload.add(new TaskUpAndLoadDto(GroupConstants.COLLECTION_METADATA_INSTANCES,
                            JsonUtil.toJsonUseJackson(meta)));
                }
                log.debug("buildConnectionMetadataPayload: connectionId={}, tableCount={}",
                        connectionId, tableMeta.size());
            } catch (Exception e) {
                log.error("buildConnectionMetadataPayload: failed for connectionId={}", connectionId, e);
            }
        }
        return payload;
    }

    /**
     * 从各类型资源的导出 payload 中提取 id → name 映射。
     * 只处理主资源文档（Task / Connection / Modules / Inspect），跳过元数据等附属条目。
     */
    private Map<String, String> buildResourceIdToNameMap(Map<String, List<TaskUpAndLoadDto>> payloadsByType) {
        Map<String, String> idToName = new HashMap<>();
        Set<String> mainCollections = new HashSet<>(Arrays.asList(
                GroupConstants.COLLECTION_TASK,
                GroupConstants.COLLECTION_CONNECTION,
                GroupConstants.COLLECTION_MODULES,
                GroupConstants.COLLECTION_INSPECT
        ));
        for (List<TaskUpAndLoadDto> items : payloadsByType.values()) {
            for (TaskUpAndLoadDto item : items) {
                if (item.getJson() == null || !mainCollections.contains(item.getCollectionName())) continue;
                String id = extractIdFromJson(item.getJson());
                String name = extractNameFromJson(item.getJson());
                if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(name)) {
                    idToName.put(id, name);
                }
            }
        }
        return idToName;
    }

    protected List<TaskUpAndLoadDto> buildGroupInfoPayload(List<GroupInfoDto> groupInfos,
                                                           Map<String, String> resourceIdToName) {
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
                // 跨环境后 id 会变，时间戳每次保存都会变，导出时剔除以避免 PR diff 噪音
                groupInfoCopy.setId(null);
                groupInfoCopy.setLastUpdAt(null);
                groupInfoCopy.setCreateAt(null);

                // Populate resource names for migration convenience
                if (CollectionUtils.isNotEmpty(groupInfoCopy.getResourceItemList()) && !resourceIdToName.isEmpty()) {
                    for (ResourceItem item : groupInfoCopy.getResourceItemList()) {
                        if (item.getId() != null) {
                            item.setName(resourceIdToName.get(item.getId()));
                        }
                    }
                }

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
        update.set("details", details);
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

    private String buildProjectName(List<GroupInfoDto> groupInfos) {
        if (CollectionUtils.size(groupInfos) == 1) {
            GroupInfoDto groupInfo = groupInfos.get(0);
            if (groupInfo != null && StringUtils.isNotBlank(groupInfo.getName())) {
                return groupInfo.getName();
            }
        }
        return GroupConstants.BATCH_EXPORT_FILE_PREFIX;
    }

    private Map<String, byte[]> buildExportContents(List<TaskUpAndLoadDto> groupInfoPayload,
            Map<String, List<TaskUpAndLoadDto>> payloadsByType) {
        Map<String, byte[]> contents = new LinkedHashMap<>();

        // GroupInfo.json — 根目录，无子目录
        contents.put("GroupInfo.json", toJsonBytes(groupInfoPayload));

        // API/MetadataDefinition.json — API 子目录
        List<TaskUpAndLoadDto> metadataDefPayload = payloadsByType.getOrDefault(
                ResourceType.METADATA_DEFINITION.name(), Collections.emptyList());
        if (!metadataDefPayload.isEmpty()) {
            contents.put("API/MetadataDefinition.json", toJsonBytes(metadataDefPayload));
        }

        // Connection/ — 每个连接独立 Config 和 Metadata 文件
        buildConnectionFileContents(payloadsByType, contents);

        // Task/ — 每个任务独立文件（包含任务配置和对应元数据）
        buildTaskFileContents(
                payloadsByType.getOrDefault(ResourceType.MIGRATE_TASK.name(), Collections.emptyList()),
                "_MigrateTask.json", contents);
        buildTaskFileContents(
                payloadsByType.getOrDefault(ResourceType.SYNC_TASK.name(), Collections.emptyList()),
                "_SyncTask.json", contents);
        buildTaskFileContents(
                payloadsByType.getOrDefault(ResourceType.SHARE_CACHE.name(), Collections.emptyList()),
                "_ShareCache.json", contents);

        // Task/ — 校验任务（每条独立文件）
        for (TaskUpAndLoadDto item : payloadsByType.getOrDefault(ResourceType.INSPECT_TASK.name(), Collections.emptyList())) {
            if (!GroupConstants.COLLECTION_INSPECT.equals(item.getCollectionName())) continue;
            String name = extractNameFromJson(item.getJson());
            if (StringUtils.isBlank(name)) continue;
            contents.put("Task/" + sanitizeFileName(name) + "_ValidateTask.json", toJsonBytes(List.of(item)));
        }

        // API/ — 每个模块独立文件
        for (TaskUpAndLoadDto item : payloadsByType.getOrDefault(ResourceType.MODULE.name(), Collections.emptyList())) {
            if (!GroupConstants.COLLECTION_MODULES.equals(item.getCollectionName())) continue;
            String name = extractNameFromJson(item.getJson());
            if (StringUtils.isBlank(name)) continue;
            contents.put("API/" + sanitizeFileName(name) + "_Module.json", toJsonBytes(List.of(item)));
        }

        return contents;
    }

    /**
     * 每个连接生成独立的 Config 和 Metadata 文件，放在 Connection/ 子目录下。
     * payload 中连接配置项（COLLECTION_CONNECTION）和元数据项（COLLECTION_METADATA_INSTANCES）
     * 通过 datasourceInstanceId 关联到各自连接。
     */
    private void buildConnectionFileContents(Map<String, List<TaskUpAndLoadDto>> payloadsByType,
            Map<String, byte[]> contents) {
        List<TaskUpAndLoadDto> payload = payloadsByType.getOrDefault(
                ResourceType.CONNECTION.name(), Collections.emptyList());
        if (payload.isEmpty()) return;

        // 第一次遍历：建立 connectionId → connectionName 映射
        Map<String, String> connIdToName = new LinkedHashMap<>();
        for (TaskUpAndLoadDto item : payload) {
            if (GroupConstants.COLLECTION_CONNECTION.equals(item.getCollectionName())) {
                String name = extractNameFromJson(item.getJson());
                String id = extractIdFromJson(item.getJson());
                if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(id)) {
                    connIdToName.put(id, name);
                }
            }
        }

        // 第二次遍历：按 source._id 将元数据分组到各连接（source._id 存储的是连接 ID）
        Map<String, List<TaskUpAndLoadDto>> metadataByConnName = new LinkedHashMap<>();
        for (TaskUpAndLoadDto item : payload) {
            if (GroupConstants.COLLECTION_METADATA_INSTANCES.equals(item.getCollectionName())) {
                String sourceId = extractNestedFieldFromJson(item.getJson(), "source", "_id");
                String connName = connIdToName.get(sourceId);
                if (connName != null) {
                    metadataByConnName.computeIfAbsent(connName, k -> new ArrayList<>()).add(item);
                }
            }
        }

        // 第三次遍历：写出每个连接的独立文件
        for (TaskUpAndLoadDto item : payload) {
            if (!GroupConstants.COLLECTION_CONNECTION.equals(item.getCollectionName())) continue;
            String name = extractNameFromJson(item.getJson());
            if (StringUtils.isBlank(name)) continue;
            String safeName = sanitizeFileName(name);
            contents.put("Connection/" + safeName + "_Connection_Config.json", toJsonBytes(List.of(item)));
            List<TaskUpAndLoadDto> metadataItems = metadataByConnName.getOrDefault(name, Collections.emptyList());
            if (!metadataItems.isEmpty()) {
                contents.put("Connection/" + safeName + "_Connection_Metadata.json", toJsonBytes(metadataItems));
            }
        }
    }

    /**
     * 按任务为单位生成独立文件（包含任务配置 + 该任务的元数据）。
     * payload 结构：[taskConfig, meta1, meta2, ..., taskConfig2, meta3, ...]
     * 每个 COLLECTION_TASK 项开启一个新的任务分组，后续非 COLLECTION_TASK 项归属该分组。
     */
    private void buildTaskFileContents(List<TaskUpAndLoadDto> payload, String suffix,
            Map<String, byte[]> contents) {
        List<TaskUpAndLoadDto> currentGroup = null;
        String currentName = null;
        for (TaskUpAndLoadDto item : payload) {
            if (GroupConstants.COLLECTION_TASK.equals(item.getCollectionName())) {
                // 保存上一个任务分组
                if (currentName != null && currentGroup != null && !currentGroup.isEmpty()) {
                    contents.put("Task/" + sanitizeFileName(currentName) + suffix, toJsonBytes(currentGroup));
                }
                currentName = extractNameFromJson(item.getJson());
                currentGroup = new ArrayList<>();
                currentGroup.add(item);
            } else if (currentGroup != null) {
                currentGroup.add(item);
            }
        }
        // 保存最后一个任务分组
        if (currentName != null && currentGroup != null && !currentGroup.isEmpty()) {
            contents.put("Task/" + sanitizeFileName(currentName) + suffix, toJsonBytes(currentGroup));
        }
    }

    /** 从 JSON 中提取 id 或 _id 字段值（支持 ObjectId 的 {$oid:...} 格式） */
    private String extractIdFromJson(String json) {
        if (StringUtils.isBlank(json)) return null;
        try {
            Map<String, Object> map = JsonUtil.parseJsonUseJackson(json, new TypeReference<>() {});
            if (map == null) return null;
            Object idObj = map.containsKey("id") ? map.get("id") : map.get("_id");
            if (idObj instanceof String s) return s;
            if (idObj instanceof Map<?, ?> m) {
                Object oid = m.get("$oid");
                if (oid instanceof String s) return s;
            }
            return idObj != null ? idObj.toString() : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    /** 从 JSON 中提取指定字段的字符串值 */
    private String extractFieldFromJson(String json, String field) {
        if (StringUtils.isBlank(json)) return null;
        try {
            Map<String, Object> map = JsonUtil.parseJsonUseJackson(json, new TypeReference<>() {});
            if (map == null) return null;
            Object val = map.get(field);
            if (val instanceof String s) return s;
            return val != null ? val.toString() : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    /** 从 JSON 中提取嵌套字段值，例如 extractNestedFieldFromJson(json, "source", "_id") */
    @SuppressWarnings("unchecked")
    private String extractNestedFieldFromJson(String json, String parentField, String childField) {
        if (StringUtils.isBlank(json)) return null;
        try {
            Map<String, Object> map = JsonUtil.parseJsonUseJackson(json, new TypeReference<>() {});
            if (map == null) return null;
            Object parent = map.get(parentField);
            if (!(parent instanceof Map)) return null;
            Object val = ((Map<String, Object>) parent).get(childField);
            if (val instanceof String s) return s;
            return val != null ? val.toString() : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    /** 将资源名称中不允许用于文件名的字符替换为下划线 */
    private String sanitizeFileName(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[/\\\\:*?\"<>|]", "_").trim();
    }

    /**
     * 将 TaskUpAndLoadDto 列表序列化为格式化 JSON 字节数组。
     * 每个 item 的 json 字段（原始转义字符串）会被反序列化为嵌套对象后再输出，
     * 避免 PR diff 中出现大量转义字符，且配合 EXPORT_MAPPER 的字段排序保证 diff 稳定。
     */
    private byte[] toJsonBytes(List<TaskUpAndLoadDto> items) {
        try {
            List<Map<String, Object>> expanded = new ArrayList<>(items.size());
            for (TaskUpAndLoadDto item : items) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put("collectionName", item.getCollectionName());
                if (item.getJson() != null) {
                    map.put("json", parseAndStripExportJson(item.getCollectionName(), item.getJson()));
                }
                expanded.add(map);
            }
            return EXPORT_MAPPER.writeValueAsBytes(expanded);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize export content to JSON", e);
        }
    }

    /**
     * 将 json 字符串解析为对象，并按集合类型剔除不应导出的运行时状态字段。
     * 解析失败时返回原始字符串（兜底）。
     */
    @SuppressWarnings("unchecked")
    private Object parseAndStripExportJson(String collectionName, String json) {
        try {
            Object parsed = EXPORT_MAPPER.readValue(json, Object.class);
            if (!(parsed instanceof Map)) return parsed;
            Map<String, Object> doc = (Map<String, Object>) parsed;
            // 各集合独有的剔除逻辑
            if (GroupConstants.COLLECTION_CONNECTION.equals(collectionName)) {
                CONNECTION_EXPORT_EXCLUDED_FIELDS.forEach(doc::remove);
            } else if (GroupConstants.COLLECTION_TASK.equals(collectionName)) {
                TASK_EXPORT_EXCLUDED_FIELDS.forEach(doc::remove);
            } else if (GroupConstants.COLLECTION_INSPECT.equals(collectionName)) {
                INSPECT_EXPORT_EXCLUDED_FIELDS.forEach(doc::remove);
            } else if (GroupConstants.COLLECTION_GROUP_INFO.equals(collectionName)) {
                doc.remove("id");
                doc.remove("createTime");
                Object gitInfoObj = doc.get("gitInfo");
                if (gitInfoObj instanceof Map) {
                    Map<String, Object> gitInfo = (Map<String, Object>) gitInfoObj;
                    BASE_DTO_VOLATILE_FIELDS.forEach(gitInfo::remove);
                }
            }
            // 通用时间戳/操作人字段：所有集合类型统一剔除
            COMMON_VOLATILE_FIELDS.forEach(doc::remove);
            return doc;
        } catch (Exception ignored) {
            return json;
        }
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
        } else if(resource instanceof InspectDto) {
            ObjectId id = ((InspectDto) resource).getId();
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
        } else if(resource instanceof InspectDto) {
            return ((InspectDto) resource).getName();
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

    /**
     * 从 payloads 中解析 MetadataDefinition，导入并返回 tagIdMap（旧ID → 新ID）。
     */
    private Map<String, String> importMetadataDefinitionsAndGetTagMap(
            Map<String, List<TaskUpAndLoadDto>> payloads, UserDetail user) {
        String metaDefFilename = ResourceType.getResourceName(ResourceType.METADATA_DEFINITION.name());
        List<TaskUpAndLoadDto> metaDefPayload = payloads.getOrDefault(metaDefFilename, Collections.emptyList());
        List<MetadataDefinitionDto> metadataDefinitions = new ArrayList<>();
        for (TaskUpAndLoadDto item : metaDefPayload) {
            if (StringUtils.isBlank(item.getJson())) continue;
            if (GroupConstants.METADATA_DEFINITION.equals(item.getCollectionName())) {
                MetadataDefinitionDto dto = JsonUtil.parseJsonUseJackson(item.getJson(), MetadataDefinitionDto.class);
                if (dto != null) metadataDefinitions.add(dto);
            }
        }
        if (metadataDefinitions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> tagMap = metadataDefinitionService.batchImport(metadataDefinitions, user);
        log.info("Imported {} MetadataDefinition entries", metadataDefinitions.size());
        return tagMap != null ? tagMap : Collections.emptyMap();
    }

    protected void checkTags(Map<ResourceType, Map<String, ?>> resourceMapsByType,Map<String,String> tagMap){
        for (Map.Entry<ResourceType, Map<String, ?>> entry : resourceMapsByType.entrySet()) {
            if(ResourceType.hasTags(entry.getKey())){
                entry.getValue().values().forEach(object -> {
                    if (object instanceof TaskDto taskDto && CollectionUtils.isNotEmpty(taskDto.getListtags())){
                        taskDto.getListtags().stream().filter(tag -> tagMap.containsKey(tag.getId())).forEach(tag -> tag.setId(tagMap.get(tag.getId())));
                    }else if(object instanceof ModulesDto modulesDto && CollectionUtils.isNotEmpty(modulesDto.getListtags())){
                        modulesDto.getListtags().stream().filter(tag -> tagMap.containsKey(tag.getId())).forEach(tag -> tag.setId(tagMap.get(tag.getId())));
                    }else if (object instanceof DataSourceConnectionDto dataSourceConnectionDto && CollectionUtils.isNotEmpty(dataSourceConnectionDto.getListtags())){
                        dataSourceConnectionDto.getListtags().stream().filter(tag -> tagMap.containsKey(tag.get("id"))).forEach(tag -> tag.put("id",tagMap.get(tag.get("id"))));
                    }else if(object instanceof InspectDto inspectDto && CollectionUtils.isNotEmpty(inspectDto.getListtags())){
                        inspectDto.getListtags().stream().filter(tag -> tagMap.containsKey(tag.getId())).forEach(tag -> tag.setId(tagMap.get(tag.getId())));
                    }
                });
            }
        }



    }

    public Page<TaskWithGroupVo> getTasksWithGroupInfo(Filter filter, UserDetail userDetail) {
        Page<TaskDto> taskPage = taskService.find(filter, userDetail);
        List<TaskDto> tasks = taskPage.getItems();
        if (CollectionUtils.isEmpty(tasks)) {
            Page<TaskWithGroupVo> result = new Page<>();
            result.setTotal(taskPage.getTotal());
            result.setItems(Collections.emptyList());
            return result;
        }

        List<String> taskIds = tasks.stream()
                .filter(t -> t.getId() != null)
                .map(t -> t.getId().toHexString())
                .collect(Collectors.toList());

        Map<String, GroupInfoDto> taskIdToGroup = buildResourceIdToGroupMap(taskIds);

        List<TaskWithGroupVo> voList = tasks.stream().map(task -> {
            TaskWithGroupVo vo = BeanUtil.deepClone(task, TaskWithGroupVo.class);
            if (task.getId() != null) {
                GroupInfoDto group = taskIdToGroup.get(task.getId().toHexString());
                if (group != null) {
                    vo.setGroupId(group.getId() != null ? group.getId().toHexString() : null);
                    vo.setGroupName(group.getName());
                }
            }
            return vo;
        }).collect(Collectors.toList());

        Page<TaskWithGroupVo> result = new Page<>();
        result.setTotal(taskPage.getTotal());
        result.setItems(voList);
        return result;
    }

    @SuppressWarnings("unchecked")
    public Page<ModuleWithGroupVo> getApisWithGroupInfo(Filter filter, UserDetail userDetail) {
        Page rawPage = modulesService.findModules(filter, userDetail);
        List<?> items = rawPage.getItems();
        if (CollectionUtils.isEmpty(items)) {
            Page<ModuleWithGroupVo> result = new Page<>();
            result.setTotal(rawPage.getTotal());
            result.setItems(Collections.emptyList());
            return result;
        }

        // 单次遍历：cast + 收集 ID + 构建 VO 列表
        List<com.tapdata.tm.modules.vo.ModulesListVo> modules = new ArrayList<>(items.size());
        List<String> moduleIds = new ArrayList<>(items.size());
        for (Object item : items) {
            if (item instanceof com.tapdata.tm.modules.vo.ModulesListVo) {
                com.tapdata.tm.modules.vo.ModulesListVo module = (com.tapdata.tm.modules.vo.ModulesListVo) item;
                modules.add(module);
                if (module.getId() != null) {
                    moduleIds.add(module.getId());
                }
            }
        }

        Map<String, GroupInfoDto> moduleIdToGroup = buildResourceIdToGroupMap(moduleIds);

        List<ModuleWithGroupVo> voList = new ArrayList<>(modules.size());
        for (com.tapdata.tm.modules.vo.ModulesListVo module : modules) {
            ModuleWithGroupVo vo = BeanUtil.deepClone(module, ModuleWithGroupVo.class);
            String id = module.getId();
            if (id != null) {
                GroupInfoDto group = moduleIdToGroup.get(id);
                if (group != null) {
                    vo.setGroupId(group.getId() != null ? group.getId().toHexString() : null);
                    vo.setGroupName(group.getName());
                }
            }
            voList.add(vo);
        }

        Page<ModuleWithGroupVo> result = new Page<>();
        result.setTotal(rawPage.getTotal());
        result.setItems(voList);
        return result;
    }

    private Map<String, GroupInfoDto> buildResourceIdToGroupMap(List<String> resourceIds) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return Collections.emptyMap();
        }
        // 转为 Set 避免内层 contains 的 O(n) 查找
        Set<String> resourceIdSet = new HashSet<>(resourceIds);
        Criteria criteria = Criteria.where("resourceItemList.id").in(resourceIdSet)
                .and("is_deleted").ne(true);
        // 只投影必要字段，减少 MongoDB 传输数据量
        Query query = new Query(criteria);
        query.fields().include("name").include("resourceItemList");
        List<GroupInfoDto> groups = findAll(query);
        Map<String, GroupInfoDto> map = new HashMap<>();
        if (CollectionUtils.isNotEmpty(groups)) {
            for (GroupInfoDto group : groups) {
                if (CollectionUtils.isNotEmpty(group.getResourceItemList())) {
                    for (ResourceItem item : group.getResourceItemList()) {
                        if (item != null && item.getId() != null && resourceIdSet.contains(item.getId())) {
                            map.put(item.getId(), group);
                        }
                    }
                }
            }
        }
        return map;
    }

}
