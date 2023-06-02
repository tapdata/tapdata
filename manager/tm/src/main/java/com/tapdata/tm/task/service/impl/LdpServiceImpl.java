package com.tapdata.tm.task.service.impl;

import com.google.common.collect.Lists;
import com.tapdata.tm.base.exception.BizException;
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
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.LdpFuzzySearchVo;
import com.tapdata.tm.task.constant.LdpDirEnum;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.utils.ThreadLocalUtils;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
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
    private MetadataInstancesService metadataInstancesService;

    private static ThreadLocal<String> tagCache = new ThreadLocal<>();

    @Autowired
    private TaskSaveService taskSaveService;

    @Autowired
    private UserService userService;

    @Override
    @Lock(value = "user.userId", type = LockType.START_LDP_FDM, expireSeconds = 15)
    public TaskDto createFdmTask(TaskDto task, UserDetail user) {
        //check fdm task
        String fdmConnId = checkFdmTask(task, user);
        task.setLdpType(TaskDto.LDP_TYPE_FDM);

        DAG dag = task.getDag();
        DatabaseNode databaseNode = (DatabaseNode) dag.getSources().get(0);
        String connectionId = databaseNode.getConnectionId();

        Criteria criteria = Criteria.where("ldpType").is(TaskDto.LDP_TYPE_FDM)
                .and("dag.nodes.connectionId").is(connectionId)
                .and("is_deleted").ne(true)
                .and("status").nin(Lists.newArrayList(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED));
        Query query = new Query(criteria);
        TaskDto oldTask = taskService.findOne(query, user);


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
                databaseNode = (DatabaseNode) task.getDag().getSources().get(0);
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
            task = createNew(task, dag, oldTask);
        }

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

        if (oldTask != null) {
            if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
                taskService.pause(taskDto, user, false, true);
            } else {
                taskService.start(taskDto, user, "00");
            }
        } else {
            taskService.start(taskDto, user, "00");
        }

        return taskDto;
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
            queryOldTask.fields().include("listtags", "qualified_name");
            oldMetaDatas = metadataInstancesService.findAllDto(queryOldTask, user);
            oldMetaMap = oldMetaDatas.stream().collect(Collectors.toMap(m -> m.getId().toHexString(), m -> m, (k1, k2) -> k1));
        }
        if (TaskDto.LDP_TYPE_FDM.equals(task.getLdpType())) {

            List<Node> sources = dag.getSources();
            Node sourceNode = sources.get(0);
            String sourceCon = ((DataParentNode) sourceNode).getConnectionId();

            Criteria criteria = Criteria.where("linkId").is(sourceCon).and("item_type").is(MetadataDefinitionDto.LDP_ITEM_FDM);
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
            metadataInstancesService.bulkUpsetByWhere(metaDatas, user);
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
    public Map<String, TaskDto> queryFdmTaskByTags(List<String> tagIds, UserDetail user) {
        Map<String, TaskDto> result = new HashMap<>();
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
            String linkId = tagMap.get(connectionId);
            if (StringUtils.isNotBlank(linkId)) {
                result.put(tagMap.get(connectionId), taskDto);
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

    private Map<String, Boolean> queryTagBelongMdm(List<String> tagIds, UserDetail user, String mdmTags) {
        Map<String, Boolean> mdmMap = new HashMap<>();

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
        List<UserDetail> userDetails = userService.loadAllUser();
        for (UserDetail userDetail : userDetails) {
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
        tasks = tasks.stream().filter(t -> isTargetNode(t.getDag(), fdmStorageConnectionId)).collect(Collectors.toList());

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
        tasks = tasks.stream().filter(t -> isTargetNode(t.getDag(), mdmStorageConnectionId)).collect(Collectors.toList());

        for (TaskDto task : tasks) {
            task.setLdpType(TaskDto.LDP_TYPE_MDM);
            createLdpMetaByTask(task, user);
            taskService.updateById(task.getId(), Update.update("ldpType", TaskDto.LDP_TYPE_MDM), user);
        }
    }

    private boolean isTargetNode(DAG dag, String fdmStorageConnectionId) {
        if (dag == null) {
            return false;
        }
        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isEmpty(targets)) {
            return false;
        }

        for (Node target : targets) {
            if (target instanceof DataParentNode) {
                if (fdmStorageConnectionId.equals(((DataParentNode<?>) target).getConnectionId())) {
                    return true;
                }
            }
        }
        return false;
    }
}
