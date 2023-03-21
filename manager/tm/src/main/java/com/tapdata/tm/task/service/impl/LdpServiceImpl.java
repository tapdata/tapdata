package com.tapdata.tm.task.service.impl;

import com.google.common.collect.Lists;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
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

    @Override
    public TaskDto createFdmTask(TaskDto task, UserDetail user) {
        //check fdm task
        String fdmConnId = checkFdmTask(task, user);
        task.setLdpType(TaskDto.LDP_TYPE_FDM);

        DAG dag = task.getDag();
        DatabaseNode databaseNode = (DatabaseNode) dag.getSources().get(0);
        String connectionId = databaseNode.getConnectionId();

        Criteria criteria = Criteria.where("ldpType").is(TaskDto.LDP_TYPE_FDM)
                .and("dag.nodes.connectionId").is(connectionId)
                .and("is_deleted").ne(true);
        Query query = new Query(criteria);
        TaskDto oldTask = taskService.findOne(query, user);


        List<String> oldTableNames = new ArrayList<>();
        if (oldTask != null) {
            flushPrefix(task.getDag(), oldTask.getDag());

            DatabaseNode oldSourceNode = (DatabaseNode) oldTask.getDag().getSources().get(0);
            List<String> tableNames = oldSourceNode.getTableNames();
            oldTableNames.addAll(tableNames);
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
        DatabaseNode target = (DatabaseNode)task.getDag().getTargets().get(0);

        List<String> targetTableNames = new ArrayList<>();
        List<SyncObjects> syncObjects = target.getSyncObjects();
        if (CollectionUtils.isNotEmpty(syncObjects)) {
            SyncObjects syncObjects1 = syncObjects.get(0);
            targetTableNames = syncObjects1.getObjectNames();
        }

        repeatTable(targetTableNames, task.getId() == null ? null : task.getId().toHexString(), fdmConnId, user);

        TaskDto taskDto;
        if (oldTask != null) {
            taskDto = taskService.updateById(task, user);
        } else {
            taskDto = taskService.confirmById(task, user, true);
        }
        //创建fdm的分类
        createFdmTags(taskDto, user);

        if (oldTask != null) {
            sourceTableNames.removeAll(oldTableNames);
            if (CollectionUtils.isNotEmpty(sourceTableNames)) {
                taskDto.setLdpNewTables(sourceTableNames);
            }
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
        MetadataDefinitionDto fdmTag = metadataDefinitionService.findOne(query);
        if (fdmTag == null) {
            throw new BizException("SystemError");
        }
        Criteria conCriteria = Criteria.where("linkId").is(connectionId)
                .and("parent_id").is(fdmTag.getId().toHexString())
                .and("item_type").is(MetadataDefinitionDto.LDP_ITEM_FDM);
        Query conTagQuery = new Query(conCriteria);
        MetadataDefinitionDto conTag = metadataDefinitionService.findOne(conTagQuery);
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
            oldTask =  task;
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
    public TaskDto createMdmTask(TaskDto task, String tagId, UserDetail user) {

        try {
            //check mdm task
            checkMdmTask(task, user);
            //add mmd type
            task.setLdpType(TaskDto.LDP_TYPE_MDM);


            if (StringUtils.isNotBlank(tagId)) {
                tagCache.set(tagId);
            }
            //create sync task
            task = taskService.confirmStart(task, user, true);
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

        Criteria metaCriteria = Criteria.where("taskId").is(task.getId().toHexString()).and("source._id").is(connectionId);
        Query query = new Query(metaCriteria);
        List<MetadataInstancesDto> metaDatas = metadataInstancesService.findAllDto(query, user);
        if (CollectionUtils.isEmpty(metaDatas)) {
            return;
        }

        if (TaskDto.LDP_TYPE_FDM.equals(task.getLdpType())) {

            List<Node> sources = dag.getSources();
            Node sourceNode = sources.get(0);
            String sourceCon = ((DataParentNode) sourceNode).getConnectionId();

            Criteria criteria = Criteria.where("linkId").is(sourceCon).and("item_type").is(MetadataDefinitionDto.LDP_ITEM_FDM);
            MetadataDefinitionDto tag = metadataDefinitionService.findOne(new Query(criteria), user);

            for (MetadataInstancesDto metaData : metaDatas) {
                buildSourceMeta(new Tag(tag.getId().toHexString(), tag.getValue()), metaData);
            }
            metadataInstancesService.bulkUpsetByWhere(metaDatas, user);
        } else {
            List<String> qualifiedNames = new ArrayList<>();
            Map<String, String> qualifiedMap = new HashMap<>();
            for (MetadataInstancesDto metaData : metaDatas) {
                String qualifiedName = metaData.getQualifiedName();
                int i = qualifiedName.lastIndexOf("_");
                String old = qualifiedName.substring(0, i);
                qualifiedNames.add(old);
                qualifiedMap.put(qualifiedName, old);
            }

            Criteria criteria = Criteria.where("qualified_name").in(qualifiedNames);
            Query query1 = new Query(criteria);
            query1.fields().include("listtags", "qualified_name");
            List<MetadataInstancesDto> oldMetas = metadataInstancesService.findAllDto(query1, user);
            Map<String, MetadataInstancesDto> oldMap = oldMetas.stream()
                    .collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, v -> v, (v1, v2) -> v1));

            List<String> tagIds = oldMetas.stream()
                    .flatMap(o -> o.getListtags() == null ? Stream.empty() : o.getListtags().stream())
                    .map(Tag::getId)
                    .collect(Collectors.toList());


            Tag mdmTag = getMdmTag();
            Map<String, Boolean> mdmMap = queryTagBelongMdm(tagIds, user, mdmTag.getId());

            Tag setTag = mdmTag;
            String tagId = tagCache.get();
            if (StringUtils.isNotBlank(tagId)) {
                MetadataDefinitionDto tag = metadataDefinitionService.findById(MongoUtils.toObjectId(tagId), user);
                if (tag != null) {
                    setTag = new Tag(tag.getId().toHexString(), tag.getValue());
                }

            }


            m :
            for (MetadataInstancesDto metaData : metaDatas) {
                String old = qualifiedMap.get(metaData.getQualifiedName());
                if (StringUtils.isNotBlank(old)) {
                    MetadataInstancesDto metadataInstancesDto = oldMap.get(old);
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

                    buildSourceMeta(setTag, metaData);
                    metadataInstancesService.save(metaData, user);

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
        List<MetadataDefinitionDto> tags = metadataDefinitionService.findAll(query);
        Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(MetadataDefinitionDto::getLinkId, v -> v.getId().toHexString(), (v1, v2) -> v1));

        Criteria criteriaTask = Criteria.where("dag.nodes.connectionId").in(tagMap.keySet()).and("ldpType").is(TaskDto.LDP_TYPE_FDM);
        Query queryTask = new Query(criteriaTask);
        List<TaskDto> taskDtos = taskService.findAllDto(queryTask, user);
        for (TaskDto taskDto : taskDtos) {
            DAG dag = taskDto.getDag();
            Node node = dag.getSources().get(0);
            String connectionId = ((DatabaseNode) node).getConnectionId();
            result.put(tagMap.get(connectionId), taskDto);
        }
        return result;
    }

    private Tag getMdmTag() {
        Criteria mdmCriteria = Criteria.where("value").is("MDM").and("parent_id").exists(false);
        Query query = new Query(mdmCriteria);
        MetadataDefinitionDto mdmTag = metadataDefinitionService.findOne(query);
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

    private static void buildSourceMeta(Tag tag, MetadataInstancesDto metaData) {
        metaData.setSourceType(SourceTypeEnum.SOURCE.name());
        metaData.setTaskId(null);
        String qualifiedName = metaData.getQualifiedName();
        int i = qualifiedName.lastIndexOf("_");
        String oldQualifiedName = qualifiedName.substring(0, i);
        metaData.setQualifiedName(oldQualifiedName);
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
        metaData.setId(null);
    }

    private void checkMdmTask(TaskDto task, UserDetail user) {
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
        TableNode target = (TableNode)targets.get(0);
        String targetConId = target.getConnectionId();

        if (!mdmConnectionId.equals(targetConId)) {
            throw new BizException("Ldp.MdmTaskTargetNotMdm");
        }


        List<Node> sources = dag.getSources();
        if (CollectionUtils.isEmpty(sources)) {
            throw new BizException("Ldp.TaskNotSource");
        }

        String tableName = target.getTableName();

        repeatTable(Lists.newArrayList(tableName), null, targetConId, user);

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
}
