package com.tapdata.tm.task.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.ResponseBody;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.schema.bean.ValidateDetail;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.LdpFuzzySearchVo;
import com.tapdata.tm.task.bean.MultiSearchDto;
import com.tapdata.tm.task.constant.LdpDirEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.utils.ThreadLocalUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.TestItem;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.security.config.web.servlet.OAuth2LoginDsl;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class LdpServiceImpl implements LdpService {

    @Autowired
    private TaskService taskService;

    @Autowired
    private LiveDataPlatformService liveDataPlatformService;


    @Autowired
    private MetadataDefinitionService metadataDefinitionService;

    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private DataSourceDefinitionService definitionService;


    @Autowired
    private MetadataInstancesService metadataInstancesService;

    private static ThreadLocal<String> tagCache = new ThreadLocal<>();

    @Autowired
    private TaskSaveService taskSaveService;

    @Autowired
    private UserService userService;

    @Autowired
    private SettingsService settingsService;

    @Autowired
    private WorkerService workerService;


    @Autowired
    private MessageQueueService messageQueueService;

    @Override
    @Lock(value = "user.userId", type = LockType.START_LDP_FDM, expireSeconds = 15)
    public TaskDto createFdmTask(TaskDto task, boolean start, UserDetail user) {
        //check fdm task
        String fdmConnId = checkFdmTask(task, user);
        task.setLdpType(TaskDto.LDP_TYPE_FDM);

        DAG dag = task.getDag();
        DatabaseNode databaseNode = (DatabaseNode) dag.getSources().get(0);
        String connectionId = databaseNode.getConnectionId();

        String type = generateLdpTaskType(connectionId, user);
        task.setType(type);


        Criteria criteria = fdmTaskCriteria(connectionId);
        criteria.and("fdmMain").is(true);
        Query query = new Query(criteria);
        TaskDto oldTask = taskService.findOne(query, user);
        if (oldTask == null) {
            criteria = fdmTaskCriteria(connectionId);
            criteria.and("name").regex("Clone_To_FDM");
            oldTask = taskService.findOne(query, user);
        }


        List<String> oldTableNames = new ArrayList<>();
        List<String> oldTargetTableNames = new ArrayList<>();
        if (oldTask != null) {
            flushPrefix(task.getDag(), oldTask.getDag());

            DatabaseNode oldSourceNode = (DatabaseNode) oldTask.getDag().getSources().get(0);
            List<String> tableNames = oldSourceNode.getTableNames();
            oldTableNames.addAll(tableNames);
            DatabaseNode oldTarget = (DatabaseNode) oldTask.getDag().getTargets().get(0);
            List<SyncObjects> syncObjects = oldTarget.getSyncObjects();
            if (CollectionUtils.isNotEmpty(syncObjects)) {
                SyncObjects syncObjects1 = syncObjects.get(0);
                oldTargetTableNames.addAll(syncObjects1.getObjectNames());
            }

            List<String> newTableNames = databaseNode.getTableNames();
            if (CollectionUtils.isNotEmpty(newTableNames)) {
                if (new HashSet<>(oldTableNames).containsAll(newTableNames)) {
                    throw new BizException("Ldp.FdmSourceTableTaskExist");
                }
            }


            if (StringUtils.isNotBlank(oldSourceNode.getTableExpression())) {
                mergeAllTable(user, connectionId, oldTask, oldTableNames);
                task = oldTask;
            } else if ((StringUtils.isNotBlank(databaseNode.getTableExpression()))) {
                mergeAllTable(user, connectionId, task, oldTableNames);
                oldTask.setDag(task.getDag());
                task = oldTask;
            } else {
                task = createNew(task, dag, oldTask);
            }
        } else if (StringUtils.isNotBlank(databaseNode.getTableExpression())) {
            mergeAllTable(user, connectionId, task, null);
        } else {
            task = createNew(task, dag, null);
        }

        task.setFdmMain(true);

        databaseNode = (DatabaseNode) task.getDag().getSources().get(0);
        List<String> sourceTableNames = new ArrayList<>(databaseNode.getTableNames());
        DatabaseNode target = (DatabaseNode) task.getDag().getTargets().get(0);

        List<String> targetTableNames = new ArrayList<>();
        List<SyncObjects> syncObjects = target.getSyncObjects();
        if (CollectionUtils.isNotEmpty(syncObjects)) {
            SyncObjects syncObjects1 = syncObjects.get(0);
            targetTableNames = syncObjects1.getObjectNames();
            targetTableNames.removeAll(oldTargetTableNames);
        }

        repeatTable(targetTableNames, task.getId() == null ? null : task.getId().toHexString(), fdmConnId, user);

        taskSaveService.supplementAlarm(task, user);

        TaskDto taskDto;
        if (oldTask != null) {
            sourceTableNames.removeAll(oldTableNames);
            if (CollectionUtils.isNotEmpty(sourceTableNames)) {
                if (CollectionUtils.isNotEmpty(oldTask.getLdpNewTables())) {
                    List<String> ldpNewTables = oldTask.getLdpNewTables();
                    ldpNewTables.addAll(sourceTableNames);
                    task.setLdpNewTables(ldpNewTables);
                } else {
                    task.setLdpNewTables(sourceTableNames);
                }
            }
            taskDto = taskService.updateById(task, user);
        } else {
            taskDto = taskService.confirmById(task, user, true);
        }
        //创建fdm的分类
        createFdmTags(taskDto, user);

        if (start) {
            if (oldTask != null) {
                if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
                    taskService.pause(taskDto, user, false, true);
                } else {
                    taskService.start(taskDto, user, "00");
                }
            } else {
                taskService.start(taskDto, user, "00");
            }
        }

        return taskDto;
    }


    public String generateLdpTaskType(String sourceConnId, UserDetail user) {
        Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(sourceConnId));
        Query conQuery = new Query(criteria);
        conQuery.fields().include("database_type", "response_body");
        DataSourceConnectionDto connection = dataSourceService.findById(MongoUtils.toObjectId(sourceConnId), "database_type", "response_body");
        dataSourceService.buildDefinitionParam(Lists.newArrayList(connection), user);
        List<Capability> capabilities = connection.getCapabilities();
        if (CollectionUtils.isEmpty(capabilities)) {
            return ParentTaskDto.TYPE_INITIAL_SYNC_CDC;
        }

        boolean streamRead = true;
        boolean batchRead = true;
        ResponseBody responseBody = connection.getResponse_body();
        if (responseBody != null) {
            List<ValidateDetail> validateDetails = responseBody.getValidateDetails();
            Map<String, ValidateDetail> map = new HashMap<>();
            if (org.apache.commons.collections.CollectionUtils.isNotEmpty(validateDetails)) {
                // list to map
                map = validateDetails.stream().collect(Collectors.toMap(ValidateDetail::getShowMsg, Function.identity()));
            }
            if (!map.containsKey(TestItem.ITEM_READ_LOG) || !"passed".equals(map.get(TestItem.ITEM_READ_LOG).getStatus())) {
                streamRead = false;
            }

            if (!map.containsKey(TestItem.ITEM_READ) || !"passed".equals(map.get(TestItem.ITEM_READ).getStatus())) {
                batchRead = false;
            }

        }
        for (Capability capability : capabilities) {
            if (!CapabilityEnum.STREAM_READ_FUNCTION.name().equalsIgnoreCase(capability.getId())) {
                streamRead = false;
            }
            if (!CapabilityEnum.BATCH_READ_FUNCTION.name().equalsIgnoreCase(capability.getId())) {
                batchRead = false;
            }
        }

        if (batchRead && streamRead) {
            return ParentTaskDto.TYPE_INITIAL_SYNC_CDC;
        }

        if (streamRead) {
            return ParentTaskDto.TYPE_CDC;
        }
        return ParentTaskDto.TYPE_INITIAL_SYNC;
    }

    private Criteria fdmTaskCriteria(String connectionId) {
        return  Criteria.where("ldpType").is(TaskDto.LDP_TYPE_FDM)
                .and("dag.nodes.connectionId").is(connectionId)
                .and("is_deleted").ne(true)
                .and("status").nin(Lists.newArrayList(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED));
    }

    @Lock(value = "user.userId", type = LockType.START_LDP_FDM, expireSeconds = 15)
    public void syncStart(UserDetail user, TaskDto taskDto) {
        if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
            taskService.pause(taskDto, user, false, true);
        } else {
            taskService.start(taskDto, user, "00");
        }
    }

    private void flushPrefix(DAG dag, DAG dag1) {
        if (dag == null) {
            return;
        }

        if (dag1 == null) {
            return;
        }

        List<Node> nodes = dag.getNodes();
        String prefix = null;
        for (Node node : nodes) {
            if (node instanceof TableRenameProcessNode) {
                prefix = ((TableRenameProcessNode) node).getPrefix();
                break;
            }
        }

        if (StringUtils.isNotBlank(prefix)) {
            List<Node> nodes1 = dag1.getNodes();
            for (Node node : nodes1) {
                if (node instanceof TableRenameProcessNode) {
                    ((TableRenameProcessNode) node).setPrefix(prefix);
                    return;
                }
            }
        }
    }

    @NotNull
    private TaskDto createNew(TaskDto task, DAG dag, TaskDto oldTask) {
        task = mergeSameSourceTask(task, oldTask);
        //add fmd type


        boolean needRename = false;
        String sourceNodeId = null;


        List<Node> nodes = dag.getNodes();
        for (Node node : nodes) {
            if (node instanceof TableRenameProcessNode) {
                LinkedHashSet<TableRenameTableInfo> tableNames1 = ((TableRenameProcessNode) node).getTableNames();
                if (CollectionUtils.isEmpty(tableNames1)) {
                    needRename = true;
                }
            }
        }

        if (needRename) {

            List<String> tableNames = new ArrayList<>();
            if (dag != null) {
                List<Node> sources = dag.getSources();
                if (CollectionUtils.isNotEmpty(sources)) {
                    Node node = sources.get(0);
                    if (node != null) {
                        sourceNodeId = node.getId();
                        tableNames = ((DatabaseNode) node).getTableNames();
                    }
                }
            }
            mergeTable(task.getDag(), sourceNodeId, tableNames);
        }
        return task;
    }

    private void mergeAllTable(UserDetail user, String connectionId, TaskDto oldTask, List<String> oldTableNames) {
        Criteria criteria1 = Criteria.where("source._id").is(connectionId)
                .and("taskId").exists(false)
                .and("is_deleted").ne(true)
                .and("meta_type").is("table")
                .and("sourceType").is(com.tapdata.tm.metadatainstance.vo.SourceTypeEnum.SOURCE);
        Query query1 = new Query(criteria1);
        query1.fields().include("original_name");
        List<MetadataInstancesDto> metadataInstancesServiceAllDto = metadataInstancesService.findAllDto(query1, user);
        List<String> tableNames = metadataInstancesServiceAllDto.stream().map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList());
        String sourceNodeId = oldTask.getDag().getSources().get(0).getId();
        if (CollectionUtils.isNotEmpty(oldTableNames)) {
            tableNames.removeAll(oldTableNames);
        }
        mergeTable(oldTask.getDag(), sourceNodeId, tableNames);
    }

    private void createFdmTags(TaskDto taskDto, UserDetail user) {
        //查询是否存在fdm下面这个数据源的分类。没有则创建。
        DAG dag = taskDto.getDag();
        List<Node> sources = dag.getSources();
        Node node = sources.get(0);
        String connectionId = ((DatabaseNode) node).getConnectionId();

        //查询fdm的顶级标签
        Criteria fdmCriteria = Criteria.where("value").is("FDM").and("parent_id").exists(false);
        Query query = new Query(fdmCriteria);
        MetadataDefinitionDto fdmTag = metadataDefinitionService.findOne(query, user);
        if (fdmTag == null) {
            throw new BizException("SystemError");
        }
        Criteria conCriteria = Criteria.where("linkId").is(connectionId)
                .and("parent_id").is(fdmTag.getId().toHexString())
                .and("item_type").is(MetadataDefinitionDto.LDP_ITEM_FDM);
        Query conTagQuery = new Query(conCriteria);
        MetadataDefinitionDto conTag = metadataDefinitionService.findOne(conTagQuery, user);
        if (conTag != null) {
            return;
        }


        Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(connectionId));
        Query conQuery = new Query(criteria);
        conQuery.fields().include("name");
        DataSourceConnectionDto connection = dataSourceService.findOne(conQuery);
        if (connection == null) {
            throw new BizException("Ldp.SourceConNotFound");
        }

        //生成当前分类下面的fdm的模型跟打上标签。
        MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
        metadataDefinitionDto.setValue(connection.getName());
        metadataDefinitionDto.setItemType(Lists.newArrayList(MetadataDefinitionDto.LDP_ITEM_FDM));
        metadataDefinitionDto.setReadOnly(true);
        metadataDefinitionDto.setLinkId(connection.getId().toHexString());

        metadataDefinitionDto.setParent_id(fdmTag.getId().toHexString());
        metadataDefinitionService.save(metadataDefinitionDto, user);
    }

    private TaskDto mergeSameSourceTask(TaskDto task, TaskDto oldTask) {
        //查询是否存在同源的fdm任务
        DAG dag = task.getDag();
        List<Node> sources = dag.getSources();
        Node node = sources.get(0);


        if (oldTask == null) {
            oldTask = task;
        }

        DAG dag1 = oldTask.getDag();
        Node node1 = dag1.getSources().get(0);

        List<String> tableNames = ((DatabaseNode) node).getTableNames();
        mergeTable(dag1, node1.getId(), tableNames);

        return oldTask;
    }

    private static void mergeTable(DAG dag1, String nodeId, List<String> tableNames) {
        if (CollectionUtils.isNotEmpty(tableNames)) {
            for (String tableName : tableNames) {
                TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
                tapCreateTableEvent.setTableId(tableName);
                try {
                    dag1.filedDdlEvent(nodeId, tapCreateTableEvent);
                } catch (Exception e) {
                    throw new BizException("SystemError");
                }
            }
        }
    }

    private String checkFdmTask(TaskDto task, UserDetail user) {
        //syncType is migrate
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(task.getSyncType())) {
            log.warn("Create fdm task, but the sync type not is migrate, sync type = {}", task.getSyncType());
            throw new BizException("Ldp.FdmSyncTypeError");
        }

        //target need fdm connection
        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        String fdmConnectionId = platformDto.getFdmStorageConnectionId();

        DAG dag = task.getDag();
        if (dag == null) {
            throw new BizException("Ldp.NewTaskDagNotFound");
        }

        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            throw new BizException("Ldp.TargetNotFound");
        }
        Node node = targets.get(0);
        String targetConId = ((DatabaseNode) node).getConnectionId();

        if (!fdmConnectionId.equals(targetConId)) {
            throw new BizException("Ldp.TargetConNotFound");
        }

        return targetConId;
    }

    @Override
    public TaskDto createMdmTask(TaskDto task, String tagId, UserDetail user, boolean confirmTable, boolean start) {

        try {
            DAG dag = task.getDag();
            if (dag != null) {
                List<Node> sourceNode = dag.getSourceNodes();
                if (sourceNode != null) {
                    Node node = sourceNode.get(0);
                    if (node instanceof TableNode) {
                        String connectionId = ((TableNode) node).getConnectionId();
                        String type = generateLdpTaskType(connectionId, user);
                        task.setType(type);
                    }
                }
            }
            taskSaveService.supplementAlarm(task, user);
            //check mdm task
            checkMdmTask(task, user, confirmTable);
            //add mmd type
            task.setLdpType(TaskDto.LDP_TYPE_MDM);


            if (StringUtils.isNotBlank(tagId)) {
                tagCache.set(tagId);
            }


            boolean hasPrimaryKey = checkNoPrimaryKey(task, user);
            //create sync task
            if (hasPrimaryKey) {
                if (start) {
                    task = taskService.confirmStart(task, user, true);
                } else {
                    task = taskService.confirmById(task, user, true);
                }
            } else {
                task = taskService.confirmById(task, user, true);
                throw new BizException("Ldp.MdmTargetNoPrimaryKey", task);
            }
        } finally {
            tagCache.remove();
        }

        return task;
    }

    @Override
    public void afterLdpTask(String taskId, UserDetail user) {
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId), user);
        taskService.updateAfter(taskDto, user);

        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        if (platformDto == null) {
            return;
        }

        String mdmStorageConnectionId = platformDto.getMdmStorageConnectionId();
        String fdmStorageConnectionId = platformDto.getFdmStorageConnectionId();

        String ldpType = ldpTask(taskDto.getDag(), fdmStorageConnectionId, mdmStorageConnectionId);
        taskDto.setLdpType(ldpType);

        if (TaskDto.LDP_TYPE_FDM.equals(ldpType)) {
            createFdmTags(taskDto, user);
        }
        createLdpMetaByTask(taskDto, user);
    }

    private boolean checkNoPrimaryKey(TaskDto taskDto, UserDetail user) {
        if (!TaskDto.LDP_TYPE_MDM.equals(taskDto.getLdpType())) {
            return true;
        }

        DAG dag = taskDto.getDag();
        if (dag == null) {
            return true;
        }

        List<Node> sources = dag.getSources();

        if (CollectionUtils.isEmpty(sources)) {
            return true;
        }


        for (Node node : sources) {
            if (node instanceof TableNode) {
                TableNode source = (TableNode) node;
                Criteria criteria = Criteria.where("source._id").is(source.getConnectionId()).and("original_name").is(source.getTableName())
                        .and("taskId").exists(false).and("is_deleted").ne(true)
                        .and("sourceType").is(SourceTypeEnum.SOURCE.name());
                Query query = new Query(criteria);
                query.fields().include("fields", "indices");

                MetadataInstancesDto meta = metadataInstancesService.findOne(query, user);

                boolean hasPrimaryKey = false;
                List<Field> fields = meta.getFields();
                if (CollectionUtils.isNotEmpty(fields)) {
                    for (Field field : fields) {
                        Boolean primaryKey = field.getPrimaryKey();
                        if (primaryKey != null && primaryKey) {
                            hasPrimaryKey = true;
                            break;
                        }
                    }
                } else {
                    hasPrimaryKey = true;
                }

                if (!hasPrimaryKey) {
                    List<TableIndex> indices = meta.getIndices();
                    if (CollectionUtils.isNotEmpty(indices)) {
                        for (TableIndex index : indices) {
                            String primaryKey = index.getPrimaryKey();
                            boolean unique = index.isUnique();
                            if (StringUtils.isNotBlank(primaryKey) || unique) {
                                hasPrimaryKey = true;
                                break;
                            }
                        }
                    }
                }

                if (!hasPrimaryKey) {
                    return false;
                }
            }
        }

        return true;
    }

    private void createLdpMetaByTask(TaskDto task, UserDetail user) {
        if (!TaskDto.LDP_TYPE_FDM.equals(task.getLdpType()) && !TaskDto.LDP_TYPE_MDM.equals(task.getLdpType())) {
            return;
        }
        DAG dag = task.getDag();
        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            return;
        }


        Node node = targets.get(0);
        String connectionId = ((DataParentNode) node).getConnectionId();

        Criteria metaCriteria = Criteria.where("taskId").is(task.getId().toHexString()).and("source._id").is(connectionId).and("nodeId").is(node.getId())
                .and("is_deleted").ne(true);
        Query query = new Query(metaCriteria);
        List<MetadataInstancesDto> metaDatas = metadataInstancesService.findAllDto(query, user);
        if (CollectionUtils.isEmpty(metaDatas)) {
            return;
        }

        Map<String, String> qualifiedMap = new HashMap<>();
        List<String> oldQualifiedNames = new ArrayList<>();
        for (MetadataInstancesDto metaData : metaDatas) {
            String oldQualifiedName = getOldQualifiedName(metaData);
            qualifiedMap.put(metaData.getQualifiedName(), oldQualifiedName);
            oldQualifiedNames.add(oldQualifiedName);
        }

        List<MetadataInstancesDto> oldMetaDatas = new ArrayList<>();
        Map<String, MetadataInstancesDto> oldMetaMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(oldQualifiedNames)) {
            Criteria criteriaOld = Criteria.where("qualified_name").in(oldQualifiedNames).and("is_deleted").ne(true);
            Query queryOldTask = new Query(criteriaOld);
            queryOldTask.fields().include("listtags", "qualified_name", "source");
            oldMetaDatas = metadataInstancesService.findAllDto(queryOldTask, user);
            oldMetaMap = oldMetaDatas.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m, (k1, k2) -> k1));
        }
        if (TaskDto.LDP_TYPE_FDM.equals(task.getLdpType())) {

            List<Node> sources = dag.getSources();
            Node sourceNode = sources.get(0);
            String sourceCon = ((DataParentNode) sourceNode).getConnectionId();

            Tag fdmTag = getfdmTag(user);
            Criteria criteria = Criteria.where("linkId").is(sourceCon).and("item_type").is(MetadataDefinitionDto.LDP_ITEM_FDM).and("parent_id").is(fdmTag.getId());
            MetadataDefinitionDto tag = metadataDefinitionService.findOne(new Query(criteria), user);
            Tag conTag = new Tag(tag.getId().toHexString(), tag.getValue());
            List<MetadataInstancesDto> saveMetaDatas = new ArrayList<>();
            for (MetadataInstancesDto metaData : metaDatas) {
                String oldQ = qualifiedMap.get(metaData.getQualifiedName());
                MetadataInstancesDto oldMeta = null;
                if (StringUtils.isNotBlank(oldQ)) {
                    oldMeta = oldMetaMap.get(oldQ);
                }

                MetadataInstancesDto metadataInstancesDto = buildSourceMeta(conTag, metaData, oldMeta);
                saveMetaDatas.add(metadataInstancesDto);
            }
            metadataInstancesService.bulkUpsetByWhere(saveMetaDatas, user);
        } else {

            List<String> tagIds = oldMetaDatas.stream()
                    .flatMap(o -> o.getListtags() == null ? Stream.empty() : o.getListtags().stream())
                    .map(Tag::getId)
                    .collect(Collectors.toList());


            Tag mdmTag = getMdmTag(user);
            Map<String, Boolean> mdmMap = queryTagBelongMdm(tagIds, user, mdmTag.getId());

            Tag setTag = mdmTag;
            String tagId = tagCache.get();
            if (StringUtils.isNotBlank(tagId)) {
                MetadataDefinitionDto tag = metadataDefinitionService.findById(MongoUtils.toObjectId(tagId), user);
                if (tag != null) {
                    setTag = new Tag(tag.getId().toHexString(), tag.getValue());
                }

            }


            m:
            for (MetadataInstancesDto metaData : metaDatas) {
                String old = qualifiedMap.get(metaData.getQualifiedName());
                if (StringUtils.isNotBlank(old)) {
                    MetadataInstancesDto metadataInstancesDto = oldMetaMap.get(old);
                    if (metadataInstancesDto != null) {
                        List<Tag> listtags = metadataInstancesDto.getListtags();
                        Update update = new Update();
                        if (CollectionUtils.isNotEmpty(listtags)) {
                            for (Tag tag : listtags) {
                                Boolean belongMdm = mdmMap.get(tag.getId());
                                if (belongMdm != null && belongMdm) {
                                    break m;
                                }
                            }

                            listtags.add(setTag);
                            update.set("listtags", listtags);
                        } else {
                            update.set("listtags", listtags);
                        }
                        metadataInstancesService.updateById(metaData.getId(), update, user);

                    }

                    metaData = buildSourceMeta(setTag, metaData);
                    metadataInstancesService.upsert(new Query(Criteria.where("qualified_name").is(metaData.getQualifiedName())), metaData, user);
                }
            }

        }
    }

    @Override
    public Map<String, List<TaskDto>> queryFdmTaskByTags(List<String> tagIds, UserDetail user) {
        Map<String, List<TaskDto>> result = new HashMap<>();
        if (CollectionUtils.isEmpty(tagIds)) {
            return result;
        }

        List<ObjectId> tagObjIds = tagIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        Criteria criteria = Criteria.where("_id").in(tagObjIds);
        Query query = new Query(criteria);
        query.fields().include("linkId");
        List<MetadataDefinitionDto> tags = metadataDefinitionService.findAllDto(query, user);
        Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(MetadataDefinitionDto::getLinkId, v -> v.getId().toHexString(), (v1, v2) -> v1));

        Criteria criteriaTask = Criteria.where("dag.nodes.connectionId").in(tagMap.keySet()).and("ldpType").is(TaskDto.LDP_TYPE_FDM);
        Query queryTask = new Query(criteriaTask);
        List<TaskDto> taskDtos = taskService.findAllDto(queryTask, user);
        for (TaskDto taskDto : taskDtos) {
            DAG dag = taskDto.getDag();
            Node node = dag.getSources().get(0);
            String connectionId = ((DatabaseNode) node).getConnectionId();
            String tagId = tagMap.get(connectionId);
            if (StringUtils.isNotBlank(tagId)) {
                List<TaskDto> tasks = result.computeIfAbsent(tagId, k -> new ArrayList<>());
                tasks.add(taskDto);
            }
        }
        return result;
    }

    public Tag getMdmTag(UserDetail user) {
        Criteria mdmCriteria = Criteria.where("value").is("MDM").and("parent_id").exists(false);
        Query query = new Query(mdmCriteria);
        MetadataDefinitionDto mdmTag = metadataDefinitionService.findOne(query, user);
        return new Tag(mdmTag.getId().toHexString(), mdmTag.getValue());
    }

    private Tag getfdmTag(UserDetail user) {
        Criteria mdmCriteria = Criteria.where("value").is("FDM").and("parent_id").exists(false);
        Query query = new Query(mdmCriteria);
        MetadataDefinitionDto mdmTag = metadataDefinitionService.findOne(query, user);
        return new Tag(mdmTag.getId().toHexString(), mdmTag.getValue());
    }

    public boolean queryTagBelongMdm(String tagId, UserDetail user, String mdmTags) {
        if (StringUtils.isBlank(mdmTags)) {
            Tag mdmTag = getMdmTag(user);
            mdmTags = mdmTag.getId();
        }
        Map<String, Boolean> map = queryTagBelongMdm(Lists.newArrayList(tagId), user, mdmTags);
        Boolean b = map.get(tagId);
        return b != null && b;

    }
    private Map<String, Boolean> queryTagBelongMdm(List<String> tagIds, UserDetail user, String mdmTags) {
        Map<String, Boolean> mdmMap = new HashMap<>();
        if (StringUtils.isBlank(mdmTags)) {
            Tag mdmTag = getMdmTag(user);
            mdmTags = mdmTag.getId();
        }
        if (CollectionUtils.isEmpty(tagIds)) {
            return mdmMap;
        }

        List<MetadataDefinitionDto> child = metadataDefinitionService.findAndChild(Lists.newArrayList(MongoUtils.toObjectId(mdmTags)), user, "_id");
        Set<String> set = child.stream().map(c -> c.getId().toHexString()).collect(Collectors.toSet());
        for (String tagId : tagIds) {
            mdmMap.put(tagId, set.contains(tagId));
        }

        return mdmMap;
    }

    private static MetadataInstancesDto buildSourceMeta(Tag tag, MetadataInstancesDto metaData) {
        return buildSourceMeta(tag, metaData, null);
    }
    private static MetadataInstancesDto buildSourceMeta(Tag tag, MetadataInstancesDto metaData, MetadataInstancesDto oldMetaData) {
        metaData.setSourceType(SourceTypeEnum.SOURCE.name());
        metaData.setTaskId(null);
        String oldQualifiedName = getOldQualifiedName(metaData);
        metaData.setQualifiedName(oldQualifiedName);

        if (oldMetaData != null) {
            metaData = oldMetaData;
        } else {
            metaData.setId(null);
        }

        metaData.setNodeId(null);
        SourceDto source = metaData.getSource();
        if (source != null) {
            String id = source.get_id();
            if (StringUtils.isNotBlank(id)) {
                List<Tag> listtags = metaData.getListtags();
                if (listtags == null) {
                    listtags = new ArrayList<>();
                    metaData.setListtags(listtags);
                }
                listtags.add(tag);
            }
        }

        return metaData;
    }

    @NotNull
    private static String getOldQualifiedName(MetadataInstancesDto metaData) {
        String qualifiedName = metaData.getQualifiedName();
        int i = qualifiedName.lastIndexOf("_");
        String oldQualifiedName = qualifiedName.substring(0, i);
        return oldQualifiedName;
    }

    private void checkMdmTask(TaskDto task, UserDetail user, boolean confirmTable) {
        //syncType is sync

        if (StringUtils.isBlank(task.getSyncType())) {
            task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        }

        if (!TaskDto.SYNC_TYPE_SYNC.equals(task.getSyncType())) {
            log.warn("Create mdm task, but the sync type not is sync, sync type = {}", task.getSyncType());
            throw new BizException("Ldp.MdmSyncTypeError");
        }

        //target need fdm connection
        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        //String fdmConnectionId = platformDto.getFdmStorageConnectionId();
        String mdmConnectionId = platformDto.getMdmStorageConnectionId();

        DAG dag = task.getDag();
        if (dag == null) {
            throw new BizException("Ldp.NewTaskDagNotFound");
        }

        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            throw new BizException("Ldp.TargetNotFound");
        }
        TableNode target = (TableNode) targets.get(0);
        String targetConId = target.getConnectionId();

        if (!mdmConnectionId.equals(targetConId)) {
            throw new BizException("Ldp.MdmTaskTargetNotMdm");
        }


        List<Node> sources = dag.getSources();
        if (CollectionUtils.isEmpty(sources)) {
            throw new BizException("Ldp.TaskNotSource");
        }

        String tableName = target.getTableName();

        if (!confirmTable) {
            repeatTable(Lists.newArrayList(tableName), null, targetConId, user);
        }

//        if (!fdmConnectionId.equals(sourceConId)) {
//            throw new BizException("");
//        }
    }


    void repeatTable(List<String> tableNames, String taskId, String connectionId, UserDetail user) {
        Criteria nin = Criteria.where("source._id").is(connectionId)
                .and("original_name").in(tableNames)
                .and("meta_type").is("table")
                .and("sourceType").is(com.tapdata.tm.metadatainstance.vo.SourceTypeEnum.VIRTUAL);
        if (StringUtils.isNotBlank(taskId)) {
            nin.and("taskId").ne(taskId);
        }
        long count = metadataInstancesService.count(new Query(nin), user);
        if (count > 0) {
            throw new BizException("Ldp.RepeatTableName");
        }
    }


    @Override
    public List<LdpFuzzySearchVo> fuzzySearch(String key, List<String> connectType, UserDetail user) {
        Criteria criteria = Criteria.where("original_name").regex(key).and("sourceType").is(SourceTypeEnum.SOURCE.name());
        if (CollectionUtils.isNotEmpty(connectType)) {
            criteria.and("source.connection_type").in(connectType);
        }

        return getLdpFuzzySearchVos(user, criteria);
    }

    @NotNull
    private List<LdpFuzzySearchVo> getLdpFuzzySearchVos(UserDetail user, Criteria criteria) {
        Query query = new Query(criteria);
        /*query.fields().include("qualified_name", "meta_type", "is_deleted", "original_name", "ancestorsName", "dev_version", "databaseId",
                "schemaVersion", "version", "comment", "name", )*/
        List<MetadataInstancesDto> metadatas = metadataInstancesService.findAllDto(query, user);


        for (MetadataInstancesDto metadata : metadatas) {
            if (metadata.getSource() != null) {
                SourceDto sourceDto = new SourceDto();
                sourceDto.setId(metadata.getSource().getId());
                sourceDto.set_id(metadata.getSource().get_id());
                metadata.setSource(sourceDto);
            }
        }
        List<LdpFuzzySearchVo> fuzzySearchList = new ArrayList<>();
        List<String> conIds = new ArrayList<>();
        for (MetadataInstancesDto metadata : metadatas) {
            if ("table".equals(metadata.getMetaType())) {
                fuzzySearchList.add(new LdpFuzzySearchVo(LdpFuzzySearchVo.FuzzyType.metadata, metadata, metadata.getSource().get_id()));
            } else if ("database".equals(metadata.getMetaType())) {
                conIds.add(metadata.getSource().get_id());
            }
        }

        Criteria criteriaCon = Criteria.where("_id").in(conIds);
        Query queryCon = new Query(criteriaCon);
        List<DataSourceConnectionDto> connections = dataSourceService.findAllDto(queryCon, user);

        for (DataSourceConnectionDto connection : connections) {
            fuzzySearchList.add(new LdpFuzzySearchVo(LdpFuzzySearchVo.FuzzyType.connection, connection, connection.getId().toHexString()));
        }

        return fuzzySearchList;
    }

    public List<LdpFuzzySearchVo> multiSearch(List<MultiSearchDto> multiSearchDtos, UserDetail loginUser) {
        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name());
        if (multiSearchDtos == null) {
            return new ArrayList<>();
        }

        List<Criteria> or = new ArrayList<>();
        for (MultiSearchDto multiSearchDto : multiSearchDtos) {
            Criteria criteriaMulti = Criteria.where("source._id").is(multiSearchDto.getConnectionId())
                    .and("original_name").in(multiSearchDto.getTableNames());
            or.add(criteriaMulti);
        }

        if (CollectionUtils.isNotEmpty(or)) {
            criteria.orOperator(or);
        }

        return getLdpFuzzySearchVos(loginUser, criteria);
    }

    @Override
    public void addLdpDirectory(UserDetail user) {
        Map<String, String> oldLdpMap = metadataDefinitionService.ldpDirKvs();
        addLdpDirectory(user, oldLdpMap);
    }


    public void addLdpDirectory(UserDetail user, Map<String, String> oldLdpMap) {
        try {
            Criteria criteria = Criteria.where("value").in(Lists.newArrayList(LdpDirEnum.LDP_DIR_SOURCE.getValue(),
                            LdpDirEnum.LDP_DIR_FDM.getValue(), LdpDirEnum.LDP_DIR_MDM.getValue(), LdpDirEnum.LDP_DIR_TARGET.getValue()))
                    .and("item_type").in(Lists.newArrayList(LdpDirEnum.LDP_DIR_SOURCE.getItemType(),
                            LdpDirEnum.LDP_DIR_FDM.getItemType(), LdpDirEnum.LDP_DIR_MDM.getItemType(), LdpDirEnum.LDP_DIR_TARGET.getItemType()))
                    .and("parent_id").exists(false);
            Query query = new Query(criteria);

            user.setAuthorities(new HashSet<>());
            List<MetadataDefinitionDto> ldpDirs = metadataDefinitionService.findAllDto(query, user);
            List<String> existValues = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(ldpDirs)) {
                existValues = ldpDirs.stream().map(MetadataDefinitionDto::getValue).collect(Collectors.toList());
            }

            Map<String, String> kvMap = Arrays.stream(LdpDirEnum.values()).collect(Collectors.toMap(LdpDirEnum::getValue, LdpDirEnum::getItemType));

            List<String> values = Arrays.stream(LdpDirEnum.values()).filter(e -> !e.equals(LdpDirEnum.LDP_DIR_API)).map(LdpDirEnum::getValue).collect(Collectors.toList());

            values.removeAll(existValues);


            List<MetadataDefinitionDto> newTags = new ArrayList<>();
            for (String value : values) {
                MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
                metadataDefinitionDto.setValue(value);
                metadataDefinitionDto.setItemType(Lists.newArrayList(kvMap.get(value)));
                newTags.add(metadataDefinitionDto);
            }

            if (CollectionUtils.isNotEmpty(newTags)) {
                List<MetadataDefinitionDto> ldpDirTags = metadataDefinitionService.save(newTags, user);

                if (oldLdpMap != null) {
                    for (MetadataDefinitionDto ldpDirTag : ldpDirTags) {
                        Update update = Update.update("parent_id", ldpDirTag.getId().toHexString());
                        String value = oldLdpMap.get(ldpDirTag.getValue());
                        if (StringUtils.isNotBlank(value)) {
                            metadataDefinitionService.update(new Query(Criteria.where("parent_id").is(oldLdpMap.get(ldpDirTag.getValue()))), update, user);
                        }
                    }
                }
            }

        } catch (Exception e) {
            log.warn("init ldp directory failed, userId = {}", user.getUserId());
        }
    }

    @Override
    public void generateLdpTaskByOld() {
        //云版不做处理
        boolean cloud = settingsService.isCloud();
        if (cloud) {
            return;
        }

        List<UserDetail> userDetails = userService.loadAllUser();
        for (UserDetail userDetail : userDetails) {
            try {
                supplementaryLdpTaskByOld(userDetail);
            } catch (Exception e) {
                log.warn("supplementary ldp task failed, user = {}, e = {}", userDetail == null ? null : userDetail.getEmail(), e);
            }

            try {
                generateFDMTaskByOld(userDetail);
            } catch (Exception e) {
                log.warn("generate fdm task by old failed, user = {}, e = {}", userDetail == null ? null : userDetail.getEmail(), e);
            }

            try {
                generateMDMTaskByOld(userDetail);
            } catch (Exception e) {
                log.warn("generate mdm task by old failed, user = {}, e = {}", userDetail == null ? null : userDetail.getEmail(), e);
            }
        }
    }

    @Override
    public Map<String, String> ldpTableStatus(String connectionId, List<String> tableNames, String ldpType, UserDetail user) {
        Map<String, String> tableStatusMap = new HashMap<>();
        if (TaskDto.LDP_TYPE_FDM.equals(ldpType)) {
            Criteria criteria = fdmTaskCriteria(connectionId);
            List<TaskDto> taskDtos = taskService.findAllDto(new Query(criteria), user);
            for (TaskDto taskDto : taskDtos) {
                DAG dag = taskDto.getDag();
                if (dag != null) {
                    LinkedList<DatabaseNode> targetNode = dag.getTargetNode();
                    DatabaseNode last = targetNode.getFirst();
                    if (last != null) {
                        List<SyncObjects> syncObjects = last.getSyncObjects();
                        SyncObjects syncObjects1 = syncObjects.get(0);
                        Map<String, String> map = new HashMap<>();
                        LinkedHashMap<String, String> tableNameRelation = syncObjects1.getTableNameRelation();
                        tableNameRelation.forEach((k, v) -> {
                            map.put(v, k);
                        });

                        List<String> ldpNewTables = taskDto.getLdpNewTables();
                        if (ldpNewTables == null) {
                            ldpNewTables = new ArrayList<>();
                        }
                        if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
                            String state = "running";
                            for (String tableName : tableNames) {
                                if (map.containsKey(tableName)) {
                                    if (!ldpNewTables.contains(map.get(tableName))) {
                                        tableStatusMap.put(tableName, state);
                                    } else {
                                        if (!"running".equals(tableStatusMap.get(tableName))) {
                                            tableStatusMap.put(tableName, "noRunning");
                                        }
                                    }
                                }
                            }
                        } else {
                            for (String tableName : tableNames) {
                                if (map.containsKey(tableName)) {
                                    if (!"running".equals(tableStatusMap.get(tableName))) {
                                        tableStatusMap.put(tableName, "noRunning");
                                    }
                                }
                            }

                        }

                    }
                }


            }

        } else {
            Criteria criteria = Criteria.where("ldpType").is(TaskDto.LDP_TYPE_MDM)
                    .and("dag.nodes.tableName").in(tableNames)
                    .and("is_deleted").ne(true)
                    .and("status").nin(Lists.newArrayList(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED));


            List<TaskDto> taskDtos = taskService.findAllDto(new Query(criteria), user);
            for (TaskDto taskDto : taskDtos) {
                DAG dag = taskDto.getDag();
                if (dag != null) {
                    List<Node> targets = dag.getTargets();
                    for (Node target : targets) {
                        if (target instanceof TableNode) {
                            String tableName = ((TableNode) target).getTableName();
                            if (tableNames.contains(tableName)) {
                                if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
                                    tableStatusMap.put(tableName, "running");
                                } else {
                                    if (!"running".equals(tableStatusMap.get(tableName))) {
                                        tableStatusMap.put(tableName, "noRunning");
                                    }
                                }
                            }

                        }
                    }
                }
            }

        }

        for (String tableName : tableNames) {
            tableStatusMap.putIfAbsent(tableName, "noRunning");
        }

        return tableStatusMap;
    }

    @Override
    public boolean checkFdmTaskStatus(String tagId, UserDetail user) {
        MetadataDefinitionDto definitionDto = metadataDefinitionService.findById(MongoUtils.toObjectId(tagId), user);
        if (definitionDto == null || StringUtils.isBlank(definitionDto.getLinkId())) {
            return true;
        }
        String connectionId = definitionDto.getLinkId();
        Criteria criteria = fdmTaskCriteria(connectionId);
        criteria.and("fdmMain").is(true);
        Query query = new Query(criteria);
        TaskDto fdmTask = taskService.findOne(query, user);
        if (fdmTask == null) {
            criteria = fdmTaskCriteria(connectionId);
            criteria.and("name").regex("Clone_To_FDM");
            fdmTask = taskService.findOne(query, user);
        }

        if (fdmTask == null) {
            return true;
        }

        switch (fdmTask.getStatus()) {
            case TaskDto.STATUS_RUNNING:
            case TaskDto.STATUS_COMPLETE:
            case TaskDto.STATUS_EDIT:
            case TaskDto.STATUS_ERROR:
            case TaskDto.STATUS_RENEW_FAILED:
            case TaskDto.STATUS_SCHEDULE_FAILED:
            case TaskDto.STATUS_STOP:
            case TaskDto.STATUS_WAIT_START:
                return false;
            default:
                return true;
        }
    }

    @Override
    public void fdmBatchStart(String tagId, List<String> taskIds, UserDetail user) {
        Map<String, List<TaskDto>> taskMap = queryFdmTaskByTags(Lists.newArrayList(tagId), user);
        List<TaskDto> taskDtos = taskMap.get(tagId);
        if (CollectionUtils.isEmpty(taskDtos)) {
            return;
        }

        if (CollectionUtils.isNotEmpty(taskIds)) {
            taskDtos = taskDtos.stream().filter(t -> taskIds.contains(t.getId().toHexString())).collect(Collectors.toList());
        }

        for (TaskDto taskDto : taskDtos) {
            switch (taskDto.getStatus()) {
                case TaskDto.STATUS_COMPLETE:
                case TaskDto.STATUS_EDIT:
                case TaskDto.STATUS_ERROR:
                case TaskDto.STATUS_RENEW_FAILED:
                case TaskDto.STATUS_SCHEDULE_FAILED:
                case TaskDto.STATUS_STOP:
                case TaskDto.STATUS_WAIT_START:
                    taskService.start(taskDto, user, "11");
                    break;
                case TaskDto.STATUS_RUNNING:
                    taskService.pause(taskDto, user, false, true);
                default:
                    log.info("fdm task status can't do anything, name = {}, status = {}", taskDto.getName(), taskDto.getStatus());

            }
        }

    }

    @Override
    public void deleteMdmTable(String id, UserDetail user) {

        MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(id), user);

        if (metadataInstancesDto == null) {
            return;
        }

        if (CollectionUtils.isEmpty(metadataInstancesDto.getListtags())) {
            throw new BizException("Ldp.DeleteMdmTableFailed");
        }

        List<String> tagIds = metadataInstancesDto.getListtags().stream().map(Tag::getId).collect(Collectors.toList());
        Map<String, Boolean> map = queryTagBelongMdm(tagIds, user, null);
        boolean belongMdm = false;
        for (String tagId : tagIds) {
            if (map.get(tagId) != null && map.get(tagId)) {
                belongMdm = true;
                break;
            }
        }
        if (!belongMdm) {
            throw new BizException("Ldp.DeleteMdmTableFailed");
        }

        LiveDataPlatformDto liveDataPlatformDto = liveDataPlatformService.findOne(new Query(), user);

        if (liveDataPlatformDto == null) {
            throw new BizException("SystemError", "Ldp config error");
        }
        String mdmStorageConnectionId = liveDataPlatformDto.getMdmStorageConnectionId();
        Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(mdmStorageConnectionId));
        Query query = new Query(criteria);
        query.fields().include("_id", "name", "config", "encryptConfig", "database_type");

        DataSourceConnectionDto connectionDto = dataSourceService.findOne(query, user);

        if (connectionDto == null) {
            throw new BizException("SystemError", "mdm connection is null");
        }
        DataSourceDefinitionDto dataSourceDefinitionDto = definitionService.getByDataSourceType(connectionDto.getDatabase_type(), user);

        if (dataSourceDefinitionDto != null) {
            connectionDto.setPdkHash(dataSourceDefinitionDto.getPdkHash());
        }


        String agent = findAgent(connectionDto, user);
        if (agent == null) {
            throw new BizException("Task.AgentNotFound");
        }

        Map<String, Object> data = new HashMap<>();
        data.put("tableName", metadataInstancesDto.getName());
        String json = JsonUtil.toJsonUseJackson(connectionDto);
        Map connections = JsonUtil.parseJsonUseJackson(json, Map.class);
        data.put("connections", connections);

        data.put("type", "dropTable");
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(agent);
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.info("build send test connection websocket context, processId = {}, userId = {}", agent, user.getUserId());
        messageQueueService.sendMessage(queueDto);
        metadataInstancesService.deleteById(MongoUtils.toObjectId(id), user);
    }

    private String findAgent(DataSourceConnectionDto connectionDto, UserDetail user) {
        if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), connectionDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(connectionDto.getAccessNodeProcessIdList())) {

            List<Worker> availableAgent = workerService.findAvailableAgent(user);
            List<String> processIds = availableAgent.stream().map(Worker::getProcessId).collect(Collectors.toList());
            String agentId = null;
            for (String p : connectionDto.getAccessNodeProcessIdList()) {
                if (processIds.contains(p)) {
                    agentId = p;
                    break;
                }
            }

            return agentId;

        } else {
            List<Worker> availableAgent = workerService.findAvailableAgent(user);
            if (CollectionUtils.isNotEmpty(availableAgent)) {
                Worker worker = availableAgent.get(0);
                return worker.getProcessId();
            }
        }
        return null;
    }


    private void supplementaryLdpTaskByOld(UserDetail user) {
        Criteria criteria = Criteria.where("ldpType").in(TaskDto.LDP_TYPE_FDM, TaskDto.LDP_TYPE_MDM)
                .and("is_deleted").ne(true);

        Query query = new Query(criteria);
        List<TaskDto> tasks = taskService.findAllDto(query, user);

        for (TaskDto task : tasks) {
            try {
                if (TaskDto.LDP_TYPE_FDM.equals(task.getLdpType())) {
                    createFdmTags(task, user);
                }
                createLdpMetaByTask(task, user);
            } catch (Exception e) {
                log.info("Supplementary ldp task exception");
            }
        }
    }
    private void generateFDMTaskByOld(UserDetail user) {
        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        if (platformDto == null) {
            return;
        }

        String fdmStorageConnectionId = platformDto.getFdmStorageConnectionId();
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(fdmStorageConnectionId)));
        query.fields().include("_id");
        DataSourceConnectionDto connectionDto = dataSourceService.findOne(query, user);
        if (connectionDto == null) {
            return;
        }
        //查询所有的fdm中间库为目标节点的复制任务。
        Criteria criteriaTask = Criteria.where("ldpType").exists(false)
                .and("syncType").is(TaskDto.SYNC_TYPE_MIGRATE)
                .and("is_deleted").ne(true)
                .and("dag.nodes.connectionId").is(fdmStorageConnectionId);
        Query queryTask = new Query(criteriaTask);
        List<TaskDto> tasks = taskService.findAllDto(queryTask, user);
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        tasks = tasks.stream().filter(t -> fdmTask(t.getDag(), fdmStorageConnectionId)).collect(Collectors.toList());

        for (TaskDto task : tasks) {
            task.setLdpType(TaskDto.LDP_TYPE_FDM);
            createFdmTags(task, user);
            createLdpMetaByTask(task, user);
            taskService.updateById(task.getId(), Update.update("ldpType", TaskDto.LDP_TYPE_FDM), user);
        }
    }



    private void generateMDMTaskByOld(UserDetail user) {
        LiveDataPlatformDto platformDto = liveDataPlatformService.findOne(new Query(), user);
        if (platformDto == null) {
            return;
        }

        String mdmStorageConnectionId = platformDto.getMdmStorageConnectionId();
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(mdmStorageConnectionId)));
        query.fields().include("_id");
        DataSourceConnectionDto connectionDto = dataSourceService.findOne(query, user);
        if (connectionDto == null) {
            return;
        }
        //查询所有的fdm中间库为目标节点的复制任务。
        Criteria criteriaTask = Criteria.where("ldpType").exists(false)
                .and("syncType").is(TaskDto.SYNC_TYPE_SYNC)
                .and("is_deleted").ne(true)
                .and("dag.nodes.connectionId").is(mdmStorageConnectionId);
        Query queryTask = new Query(criteriaTask);
        List<TaskDto> tasks = taskService.findAllDto(queryTask, user);
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        tasks = tasks.stream().filter(t -> mdmTask(t.getDag(), mdmStorageConnectionId)).collect(Collectors.toList());

        for (TaskDto task : tasks) {
            task.setLdpType(TaskDto.LDP_TYPE_MDM);
            createLdpMetaByTask(task, user);
            taskService.updateById(task.getId(), Update.update("ldpType", TaskDto.LDP_TYPE_MDM), user);
        }
    }

    private boolean fdmTask(DAG dag, String fdmStorageConnectionId) {
        String ldpType = ldpTask(dag, fdmStorageConnectionId, null);
        return TaskDto.LDP_TYPE_FDM.equals(ldpType);
    }

    private boolean mdmTask(DAG dag, String mdmStorageConnectionId) {
        String ldpType = ldpTask(dag, null, mdmStorageConnectionId);
        return TaskDto.LDP_TYPE_MDM.equals(ldpType);
    }

    private String ldpTask(DAG dag, String fdmStorageConnectionId, String mdmStorageConnectionId) {
        if (dag == null) {
            return null;
        }
        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            return null;
        }

        for (Node target : targets) {
            if (target instanceof DataParentNode) {
                if (target instanceof DatabaseNode && fdmStorageConnectionId != null && fdmStorageConnectionId.equals(((DataParentNode<?>) target).getConnectionId())) {
                    return TaskDto.LDP_TYPE_FDM;
                }
                if (target instanceof TableNode && mdmStorageConnectionId != null && mdmStorageConnectionId.equals(((DataParentNode<?>) target).getConnectionId())) {
                    return TaskDto.LDP_TYPE_MDM;
                }
            }
        }
        return null;
    }
}
