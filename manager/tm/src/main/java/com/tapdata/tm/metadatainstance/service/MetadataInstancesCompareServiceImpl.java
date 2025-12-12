package com.tapdata.tm.metadatainstance.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import com.tapdata.manager.common.utils.StringUtils;
import java.util.function.Function;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadataInstancesCompare.param.MetadataInstancesApplyParam;
import com.tapdata.tm.metadataInstancesCompare.repository.MetadataInstancesCompareRepository;
import com.tapdata.tm.metadataInstancesCompare.service.MetadataInstancesCompareService;
import com.tapdata.tm.metadatainstance.vo.MetadataInstancesCompareResult;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaAsyncService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MetadataInstancesCompareServiceImpl extends MetadataInstancesCompareService {
    public MetadataInstancesCompareServiceImpl(@NonNull MetadataInstancesCompareRepository repository) {
        super(repository);
    }
    private MetadataInstancesService metadataInstancesService;
    private TaskService taskService;

    @Override
    public void saveMetadataInstancesCompareApply(List<MetadataInstancesApplyParam> metadataInstancesApplyParams, UserDetail userDetail, Boolean all, String nodeId) {
        if (all) {
            handleSaveAllApply(nodeId, userDetail);
        } else {
            handleSavePartialApply(metadataInstancesApplyParams, nodeId, userDetail);
        }
    }

    /**
     * 处理保存所有应用配置
     */
    private void handleSaveAllApply(String nodeId, UserDetail userDetail) {
        // 查询所有比较数据
        Query compareQuery = Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE).and("differenceFieldList").ne(new ArrayList<>()));
        List<MetadataInstancesCompareDto> compareDtos = findAll(compareQuery);

        if (CollectionUtils.isEmpty(compareDtos)) {
            return;
        }

        // 删除现有的应用配置
        Query applyQuery = Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));
        deleteAll(applyQuery);

        // 转换为应用配置并保存
        List<MetadataInstancesCompareDto> applyDtos = convertToApplyDtos(compareDtos);
        save(applyDtos, userDetail);
        getProcessMetadataInstances(applyDtos, true, userDetail,null);
    }

    /**
     * 处理保存部分应用配置
     */
    private void handleSavePartialApply(List<MetadataInstancesApplyParam> metadataInstancesApplyParams, String nodeId, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(metadataInstancesApplyParams)) {
            return;
        }

        // 根据参数获取要应用的比较数据
        List<MetadataInstancesCompareDto> newApplyDtos = getMetadataInstancesCompareDtosByApplyParams(metadataInstancesApplyParams, nodeId);
        if (CollectionUtils.isEmpty(newApplyDtos)) {
            return;
        }

        // 转换为应用配置
        List<MetadataInstancesCompareDto> convertedApplyDtos = convertToApplyDtos(newApplyDtos);

        // 处理与现有应用配置的合并
        mergeWithExistingApplyDtos(convertedApplyDtos, metadataInstancesApplyParams, nodeId, userDetail);

        // 处理元数据实例
        getProcessMetadataInstances(convertedApplyDtos, true, userDetail,null);
    }

    /**
     * 转换为应用配置DTO
     */
    private List<MetadataInstancesCompareDto> convertToApplyDtos(List<MetadataInstancesCompareDto> compareDtos) {
        return compareDtos.stream()
                .map(dto -> {
                    dto.setId(null);
                    dto.setType(MetadataInstancesCompareDto.TYPE_APPLY);
                    dto.setDifferenceFieldList(dto.getDifferenceFieldList().stream().filter(differenceField -> !(differenceField.getType() == DifferenceTypeEnum.PrimaryKeyInconsistency)).collect(Collectors.toList()));
                    return dto;
                })
                .collect(Collectors.toList());
    }

    /**
     * 与现有应用配置合并
     */
    private List<MetadataInstancesCompareDto> mergeWithExistingApplyDtos(List<MetadataInstancesCompareDto> newApplyDtos,
                                                                        List<MetadataInstancesApplyParam> applyParams,
                                                                        String nodeId,
                                                                        UserDetail userDetail) {
        // 查询现有的应用配置
        List<String> tableNames = newApplyDtos.stream()
                .map(MetadataInstancesCompareDto::getTableName)
                .collect(Collectors.toList());

        Query existingQuery = Query.query(Criteria.where("nodeId").is(nodeId)
                .and("tableName").in(tableNames)
                .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));

        List<MetadataInstancesCompareDto> existingApplyDtos = findAll(existingQuery);

        if (CollectionUtils.isEmpty(existingApplyDtos)) {
            // 没有现有配置，直接保存新配置
            save(newApplyDtos, userDetail);
            return newApplyDtos;
        } else {
            // 有现有配置，需要合并
            List<MetadataInstancesCompareDto> mergedApplyDtos = performMergeOperation(newApplyDtos, existingApplyDtos, applyParams);

            // 删除旧配置，保存合并后的配置
            deleteAll(existingQuery);
            save(mergedApplyDtos, userDetail);
            return mergedApplyDtos;
        }
    }

    /**
     * 执行合并操作
     */
    private List<MetadataInstancesCompareDto> performMergeOperation(List<MetadataInstancesCompareDto> newApplyDtos,
                                                                   List<MetadataInstancesCompareDto> existingApplyDtos,
                                                                   List<MetadataInstancesApplyParam> applyParams) {
        // 构建应用字段映射
        Map<String, Set<String>> applyFieldsMap = applyParams.stream()
                .filter(param -> CollectionUtils.isNotEmpty(param.getFieldNames()))
                .collect(Collectors.toMap(
                        MetadataInstancesApplyParam::getQualifiedName,
                        param -> new HashSet<>(param.getFieldNames()),
                        (existing, replacement) -> {
                            existing.addAll(replacement);
                            return existing;
                        }
                ));

        // 构建现有字段映射
        Map<String, List<DifferenceField>> existingFieldsMap = existingApplyDtos.stream()
                .collect(Collectors.toMap(
                        MetadataInstancesCompareDto::getQualifiedName,
                        MetadataInstancesCompareDto::getDifferenceFieldList,
                        (existing, replacement) -> existing
                ));

        // 执行合并逻辑
        newApplyDtos.forEach(newDto -> {
            String qualifiedName = newDto.getQualifiedName();
            List<DifferenceField> existingFields = existingFieldsMap.get(qualifiedName);
            Set<String> applyFieldNames = applyFieldsMap.get(qualifiedName);

            if (existingFields != null && applyFieldNames != null) {
                // 过滤出不在新应用字段中的现有字段
                List<DifferenceField> fieldsToKeep = existingFields.stream()
                        .filter(field -> !applyFieldNames.contains(field.getColumnName()))
                        .collect(Collectors.toList());

                // 将过滤后的字段添加到新DTO中
                if (CollectionUtils.isNotEmpty(fieldsToKeep)) {
                    newDto.getDifferenceFieldList().addAll(fieldsToKeep);
                }
            }

            // 重置ID以便保存
            newDto.setId(null);
        });

        return newApplyDtos;
    }

    @Override
    public void deleteMetadataInstancesCompareApply(List<MetadataInstancesApplyParam> metadataInstancesApplyParams, UserDetail userDetail, Boolean all,Boolean invalid, String nodeId) {
        if (all && Boolean.TRUE.equals(invalid)) {
            handleDeleteInvalidApply(nodeId, userDetail);
        }else if(all){
            handleDeleteAllApply(nodeId, userDetail);
        } else {
            handleDeletePartialApply(metadataInstancesApplyParams, nodeId, userDetail);
        }
    }

    private void handleDeleteInvalidApply(String nodeId, UserDetail userDetail) {
        List<MetadataInstancesCompareDto> compareDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                .and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE)));
        List<MetadataInstancesCompareDto> applyDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY)));
        List<MetadataInstancesCompareDto> invalidApplyDtos = getInvalidApplyDtos(compareDtos, applyDtos,null,null);
        if(CollectionUtils.isNotEmpty(invalidApplyDtos)){
            Map<String, List<String>> removeFieldsMap = invalidApplyDtos.stream()
                    .collect(Collectors.toMap(
                            MetadataInstancesCompareDto::getQualifiedName,
                            param -> Optional.ofNullable(param.getDifferenceFieldList()).stream().flatMap(Collection::stream).map(DifferenceField::getColumnName).collect(Collectors.toList()),
                            (existing, replacement) -> existing
                    ));
            Query query = Query.query(Criteria.where("nodeId").is(nodeId)
                    .and("qualifiedName").in(invalidApplyDtos.stream().map(MetadataInstancesCompareDto::getQualifiedName).collect(Collectors.toList()))
                    .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));
            ProcessResult processResult = processFieldDeletion(applyDtos, removeFieldsMap);
            deleteAll(query);
            if (CollectionUtils.isNotEmpty(processResult.getUpdatedCompareDtos())) {
                save(processResult.getUpdatedCompareDtos(), userDetail);
            }
        }
    }

    /**
     * 处理删除所有应用配置
     */
    private void handleDeleteAllApply(String nodeId, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));
        List<MetadataInstancesCompareDto> compareDtos = findAll(query);
        if (CollectionUtils.isEmpty(compareDtos)) {
            throw new BizException("metadatainstances.compare.undo.configuration");
        }
        deleteAll(query);
        getProcessMetadataInstances(compareDtos, false, userDetail,getApplyRules(nodeId,compareDtos.get(0).getTaskId()));
    }

    /**
     * 处理删除部分应用配置
     */
    private void handleDeletePartialApply(List<MetadataInstancesApplyParam> metadataInstancesApplyParams, String nodeId, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(metadataInstancesApplyParams)) {
            return;
        }

        // 构建查询条件和字段映射
        List<String> qualifiedNames = metadataInstancesApplyParams.stream()
                .map(MetadataInstancesApplyParam::getQualifiedName)
                .collect(Collectors.toList());

        Map<String, List<String>> removeFieldsMap = metadataInstancesApplyParams.stream()
                .collect(Collectors.toMap(
                        MetadataInstancesApplyParam::getQualifiedName,
                        param -> Optional.ofNullable(param.getFieldNames()).orElse(Collections.emptyList()),
                        (existing, replacement) -> existing // 处理重复key的情况
                ));

        // 查询现有数据
        Query query = Query.query(Criteria.where("nodeId").is(nodeId)
                .and("qualifiedName").in(qualifiedNames)
                .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));

        List<MetadataInstancesCompareDto> existingCompareDtos = findAll(query);
        if (CollectionUtils.isEmpty(existingCompareDtos)) {
            throw new BizException("metadatainstances.compare.undo.configuration");
        }

        // 处理字段删除逻辑
        ProcessResult processResult = processFieldDeletion(existingCompareDtos, removeFieldsMap);

        // 执行数据库操作
        deleteAll(query);
        if (CollectionUtils.isNotEmpty(processResult.getUpdatedCompareDtos())) {
            save(processResult.getUpdatedCompareDtos(), userDetail);
        }

        // 处理元数据实例
        if (CollectionUtils.isNotEmpty(processResult.getRemovedCompareDtos())) {
            getProcessMetadataInstances(processResult.getRemovedCompareDtos(), false, userDetail,getApplyRules(nodeId,processResult.getRemovedCompareDtos().get(0).getTaskId()));
        }
    }

    /**
     * 处理字段删除逻辑
     */
    private ProcessResult processFieldDeletion(List<MetadataInstancesCompareDto> compareDtos, Map<String, List<String>> removeFieldsMap) {
        List<MetadataInstancesCompareDto> updatedCompareDtos = new ArrayList<>();
        List<MetadataInstancesCompareDto> removedCompareDtos = new ArrayList<>();

        for (MetadataInstancesCompareDto compareDto : compareDtos) {
            String qualifiedName = compareDto.getQualifiedName();
            List<String> fieldsToRemove = removeFieldsMap.get(qualifiedName);

            if (CollectionUtils.isEmpty(compareDto.getDifferenceFieldList())) {
                // 如果没有要删除的字段或者原本就没有字段，跳过处理
                continue;
            }

            // 分离要保留和要删除的字段
            List<DifferenceField> fieldsToKeep ;
            List<DifferenceField> fieldsToDelete;
            if(CollectionUtils.isNotEmpty(fieldsToRemove)){
                Map<Boolean, List<DifferenceField>> partitionedFields = compareDto.getDifferenceFieldList().stream()
                        .collect(Collectors.partitioningBy(field -> fieldsToRemove.contains(field.getColumnName())));
                fieldsToKeep = partitionedFields.get(false);
                fieldsToDelete = partitionedFields.get(true);
            }else{
                fieldsToKeep = new ArrayList<>();
                fieldsToDelete = compareDto.getDifferenceFieldList();
            }


            // 创建删除记录
            if (CollectionUtils.isNotEmpty(fieldsToDelete)) {
                MetadataInstancesCompareDto removedDto = BeanUtil.copyProperties(compareDto, MetadataInstancesCompareDto.class);
                removedDto.setDifferenceFieldList(fieldsToDelete);
                removedCompareDtos.add(removedDto);
            }

            // 创建更新记录（如果还有要保留的字段）
            if (CollectionUtils.isNotEmpty(fieldsToKeep)) {
                compareDto.setId(null); // 重置ID以便重新保存
                compareDto.setDifferenceFieldList(fieldsToKeep);
                updatedCompareDtos.add(compareDto);
            }
        }

        return new ProcessResult(updatedCompareDtos, removedCompareDtos);
    }

    /**
     * 处理结果内部类
     */
    private static class ProcessResult {
        private final List<MetadataInstancesCompareDto> updatedCompareDtos;
        private final List<MetadataInstancesCompareDto> removedCompareDtos;

        public ProcessResult(List<MetadataInstancesCompareDto> updatedCompareDtos, List<MetadataInstancesCompareDto> removedCompareDtos) {
            this.updatedCompareDtos = updatedCompareDtos;
            this.removedCompareDtos = removedCompareDtos;
        }

        public List<MetadataInstancesCompareDto> getUpdatedCompareDtos() {
            return updatedCompareDtos;
        }

        public List<MetadataInstancesCompareDto> getRemovedCompareDtos() {
            return removedCompareDtos;
        }
    }

    @Override
    public MetadataInstancesCompareResult getMetadataInstancesCompareResult(String nodeId,String taskId,String tableFilter,int page, int pageSize,List<String> types) {
        Criteria criteria = Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_STATUS);
        Query query = Query.query(criteria);
        MetadataInstancesCompareDto metadataInstancesCompareStatus = findOne(query);
        if (metadataInstancesCompareStatus == null) {return null;}
        MetadataInstancesCompareResult metadataInstancesCompareResult = new MetadataInstancesCompareResult();
        metadataInstancesCompareResult.setStatus(metadataInstancesCompareStatus.getStatus());
        if (metadataInstancesCompareStatus.getStatus().equals(MetadataInstancesCompareDto.STATUS_DONE)) {
            metadataInstancesCompareResult.setFinishTime(metadataInstancesCompareStatus.getLastUpdAt());
            Criteria where = Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE).and("differenceFieldList").ne(new ArrayList<>());
            if (StringUtils.isNotBlank(tableFilter)) {
                Pattern pattern = Pattern.compile(tableFilter, Pattern.CASE_INSENSITIVE);
                where.and("tableName").regex(pattern);
            }
            List<MetadataInstancesCompareDto> compareDtos;
            if(CollectionUtils.isNotEmpty(types)){
                if(types.contains(DifferenceTypeEnum.PrimaryKeyInconsistency.name())){
                    where.andOperator(
                            new Criteria().orOperator(
                                    Criteria.where("differenceFieldList").elemMatch(Criteria.where("type").in(types)),
                                    Criteria.where("differenceFieldList").elemMatch(Criteria.where("isPrimaryKey").is(true))
                            )
                    );
                }else{
                    where.and("differenceFieldList.type").in(types);
                }
                compareDtos = geMetadataInstancesCompareDtoByType(nodeId,page,pageSize,types,tableFilter);
            }else{
                Query pageQuery = Query.query(where);
                if (pageSize > 0) {
                    pageQuery.skip((long) (Math.max(1, page) - 1) * pageSize);
                    pageQuery.limit(pageSize);
                }
                compareDtos = findAll(pageQuery);
            }
            List<String> applyRules = getApplyRules(nodeId,taskId);
            long totals = count(Query.query(where));
            metadataInstancesCompareResult.setCompareDtos(new Page<>(Math.max(totals, compareDtos.size()), compareDtos));
            List<MetadataInstancesCompareDto> applyDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                    .and("qualifiedName").in(compareDtos.stream().map(MetadataInstancesCompareDto::getQualifiedName).collect(Collectors.toList()))
                    .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY)));
            getInvalidApplyDtos(compareDtos, applyDtos,applyRules,null);
            metadataInstancesCompareResult.setInvalidApplyDtos(getAllInvalidApplyDtos(nodeId,applyRules,metadataInstancesCompareResult));
        }

        return metadataInstancesCompareResult;
    }

    @Override
    public List<String> getApplyRules(String nodeId, String taskId) {
        if(StringUtils.isBlank(taskId))return new ArrayList<>();
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId)));
        query.fields().include("dag");
        TaskDto taskDto = taskService.findOne(query);
        if(taskDto == null)return new ArrayList<>();
        if (!(taskDto.getDag().getNode(nodeId) instanceof DataParentNode<?>)) {
            return new ArrayList<>();
        }
        DataParentNode targetNode = (DataParentNode)taskDto.getDag().getNode(nodeId);
        if(targetNode.getApplyCompareRule()){
            return targetNode.getApplyCompareRules();
        } else {
            return new ArrayList<>();
        }
    }

    public List<MetadataInstancesCompareDto> geMetadataInstancesCompareDtoByType(String nodeId,Integer page,Integer pageSize,List<String> types,String tableFilter) {
        Criteria criteria = Criteria.where("nodeId").is(nodeId)
                .and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE);
        Document cond;
        if(types.contains(DifferenceTypeEnum.PrimaryKeyInconsistency.name())){
            criteria.andOperator(
                    new Criteria().orOperator(
                            Criteria.where("differenceFieldList").elemMatch(Criteria.where("type").in(types)),
                            Criteria.where("differenceFieldList").elemMatch(Criteria.where("isPrimaryKey").is(true))
                    ));
            cond = new Document("$or", Arrays.asList(new Document("$in", Arrays.asList("$$item.type", types)), new Document("$eq", Arrays.asList("$$item.isPrimaryKey", true))));
        }else{
            criteria.and("differenceFieldList.type").in(types);
            cond = new Document("$and", Arrays.asList(new Document("$in", Arrays.asList("$$item.type", types))));
        }

        if (StringUtils.isNotBlank(tableFilter)) {
            Pattern pattern = Pattern.compile(tableFilter, Pattern.CASE_INSENSITIVE);
            criteria.and("tableName").regex(pattern);
        }
        MatchOperation matchOperation = Aggregation.match(criteria);
        ProjectionOperation projectionOperation = Aggregation.project()
                .and("nodeId").as("nodeId")
                .and("tableName").as("tableName")
                .and("qualifiedName").as("qualifiedName")
                .and("type").as("type")
                .and(context -> new Document("$filter", new Document()
                        .append("input", "$differenceFieldList")
                        .append("as", "item")
                        .append("cond", cond)))
                .as("differenceFieldList");
        Aggregation aggregation;
        if(pageSize > 0){
            long skip = (long) (Math.max(1, page) - 1) * pageSize;
            aggregation = Aggregation.newAggregation(
                    matchOperation,
                    projectionOperation,
                    Aggregation.skip(skip),
                    Aggregation.limit(pageSize)
            );
        } else {
            aggregation = Aggregation.newAggregation(matchOperation, projectionOperation);
        }
        return repository.getMongoOperations()
                .aggregate(aggregation, "MetadataInstancesCompare", MetadataInstancesCompareDto.class)
                .getMappedResults();
    }

    @Override
    public Map<String,List<DifferenceField>> getMetadataInstancesComparesByType(String nodeId, List<String> types) {
        List<MetadataInstancesCompareDto> autoApplyDtos = null;
        if(CollectionUtils.isNotEmpty(types)){
            autoApplyDtos = geMetadataInstancesCompareDtoByType(nodeId, 0, 0, types,null);
        }
        List<MetadataInstancesCompareDto> userApplyDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_APPLY)));

        Map<String,List<DifferenceField>> differenceFieldMap;
        if(CollectionUtils.isNotEmpty(userApplyDtos)){
            differenceFieldMap = userApplyDtos.stream().collect(Collectors.toMap(MetadataInstancesCompareDto::getQualifiedName, MetadataInstancesCompareDto::getDifferenceFieldList));
        }else{
            differenceFieldMap = new HashMap<>();
        }
        if(CollectionUtils.isNotEmpty(autoApplyDtos)){
            autoApplyDtos.forEach(autoApplyDto -> {
                if(differenceFieldMap.containsKey(autoApplyDto.getQualifiedName())){
                    Set<DifferenceField> differenceFieldSet = new HashSet<>(differenceFieldMap.get(autoApplyDto.getQualifiedName()));
                    differenceFieldSet.addAll(autoApplyDto.getDifferenceFieldList());
                    differenceFieldMap.put(autoApplyDto.getQualifiedName(), new ArrayList<>(differenceFieldSet));
                }else{
                    differenceFieldMap.put(autoApplyDto.getQualifiedName(), autoApplyDto.getDifferenceFieldList());
                }
            });
        }
        return differenceFieldMap;
    }

    @Override
    public MetadataInstancesCompareResult compareAndGetMetadataInstancesCompareResult(String nodeId, String taskId, UserDetail userDetail, Boolean isSave) {
        ComparisonContext context = validateAndPrepareContext(nodeId, taskId, userDetail);
        if (context == null) {
            return new MetadataInstancesCompareResult();
        }
        if (needsRecomparison(context)) {
            return performComparison(context, userDetail, isSave);
        } else if (isComparisonDone(context.getCompareStatus())) {
            return buildExistingComparisonResult(context);
        }

        return new MetadataInstancesCompareResult();
    }

    /**
     * Validates prerequisites and prepares comparison context
     */
    private ComparisonContext validateAndPrepareContext(String nodeId, String taskId, UserDetail userDetail) {
        // Get comparison status
        MetadataInstancesCompareDto compareStatus = findOne(
            Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_STATUS))
        );

        // Get task and validate
        Query taskQuery = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId)));
        taskQuery.fields().include("dag");
        TaskDto taskDto = taskService.findOne(taskQuery, userDetail);
        if (taskDto == null) {
            return null;
        }

        if (!(taskDto.getDag().getNode(nodeId) instanceof DataParentNode<?>)) {
            return null;
        }
        DataParentNode targetNode = (DataParentNode) taskDto.getDag().getNode(nodeId);

        if (shouldSkipComparison(targetNode)) {
            return null;
        }

        // Get metadata instances
        List<MetadataInstancesDto> deductionMetadataInstances = metadataInstancesService.findByNodeId(
            nodeId, userDetail, taskId, "original_name", "fields", "qualified_name", "name", "source._id", "last_updated"
        );
        if (CollectionUtils.isEmpty(deductionMetadataInstances)) {
            return null;
        }

        // Get apply rules and target schema load time
        List<String> applyRules = getApplyRules(nodeId, taskId);
        Long targetSchemaLoadTime = metadataInstancesService.findDatabaseMetadataInstanceLastUpdate(
            targetNode.getConnectionId(), userDetail
        );

        // Get apply DTOs
        List<MetadataInstancesCompareDto> applyDtos = findAll(
            Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_APPLY))
        );

        return ComparisonContext.builder()
            .nodeId(nodeId)
            .taskId(taskId)
            .taskDto(taskDto)
            .connectionId(targetNode.getConnectionId())
            .compareStatus(compareStatus)
            .deductionMetadataInstances(deductionMetadataInstances)
            .applyRules(applyRules)
            .targetSchemaLoadTime(targetSchemaLoadTime)
            .applyDtos(applyDtos).compareIgnoreCase(targetNode.getCompareIgnoreCase())
            .build();
    }

    /**
     * Checks if comparison should be skipped due to schema-free connection
     */
    private boolean shouldSkipComparison(DataParentNode targetNode) {
        if (null == targetNode.getAttrs()) {
            return false;
        }
        List<String> connectionTags = (List<String>) targetNode.getAttrs().get("connectionTags");
        return CollectionUtils.isNotEmpty(connectionTags) && connectionTags.contains("schema-free");
    }

    /**
     * Determines if recomparison is needed based on schema load time or data changes
     */
    private boolean needsRecomparison(ComparisonContext context) {
        MetadataInstancesCompareDto compareStatus = context.getCompareStatus();
        Long targetSchemaLoadTime = context.getTargetSchemaLoadTime();
        List<MetadataInstancesDto> deductionMetadataInstances = context.getDeductionMetadataInstances();

        // No previous comparison status
        if (compareStatus == null) {
            return true;
        }

        // Schema has been updated since last comparison
        if (isSchemaUpdatedSinceLastComparison(compareStatus, targetSchemaLoadTime)) {
            return true;
        }

        // Number of metadata instances has changed
        return hasMetadataInstanceCountChanged(context.getNodeId(), deductionMetadataInstances);
    }

    /**
     * Checks if schema was updated since last comparison
     */
    private boolean isSchemaUpdatedSinceLastComparison(MetadataInstancesCompareDto compareStatus, Long targetSchemaLoadTime) {
        return compareStatus.getTargetSchemaLoadTime() != null &&
               targetSchemaLoadTime != null &&
               compareStatus.getStatus().equals(MetadataInstancesCompareDto.STATUS_DONE) &&
               targetSchemaLoadTime > compareStatus.getTargetSchemaLoadTime().getTime();
    }

    /**
     * Checks if the number of metadata instances has changed
     */
    private boolean hasMetadataInstanceCountChanged(String nodeId, List<MetadataInstancesDto> deductionMetadataInstances) {
        List<String> qualifiedNames = deductionMetadataInstances.stream()
            .map(MetadataInstancesDto::getQualifiedName)
            .collect(Collectors.toList());

        long oldCompareDtoSize = count(Query.query(
            Criteria.where("nodeId").is(nodeId)
                .and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE)
                .and("qualifiedName").in(qualifiedNames)
        ));

        return deductionMetadataInstances.size() != oldCompareDtoSize;
    }

    /**
     * Checks if comparison is already done
     */
    private boolean isComparisonDone(MetadataInstancesCompareDto compareStatus) {
        return compareStatus != null && compareStatus.getStatus().equals(MetadataInstancesCompareDto.STATUS_DONE);
    }

    /**
     * Performs the actual comparison and returns the result
     */
    private MetadataInstancesCompareResult performComparison(ComparisonContext context, UserDetail userDetail, Boolean isSave) {
        // Get apply fields and prepare data structures
        Map<String, List<DifferenceField>> applyFields = getMetadataInstancesComparesByType(
            context.getNodeId(), context.getApplyRules()
        );

        Map<String, MetadataInstancesDto> deductionMap = context.getDeductionMetadataInstances().stream()
            .collect(Collectors.toMap(MetadataInstancesDto::getName, Function.identity()));

        List<String> tableNames = context.getDeductionMetadataInstances().stream()
            .map(MetadataInstancesDto::getName)
            .collect(Collectors.toList());

        // Get target metadata instances
        List<MetadataInstancesDto> targetMetadataInstances;
        if(context.getCompareIgnoreCase()){
            targetMetadataInstances = metadataInstancesService.findSourceSchemaBySourceIdIgnoreCase(
                    context.getConnectionId(), tableNames, userDetail,
                    "original_name", "fields", "qualified_name", "name", "source._id", "last_updated"
            );
        }else{
            targetMetadataInstances = metadataInstancesService.findSourceSchemaBySourceId(
                    context.getConnectionId(), tableNames, userDetail,
                    "original_name", "fields", "qualified_name", "name", "source._id", "last_updated"
            );
        };

        if (CollectionUtils.isEmpty(targetMetadataInstances)) {
            return createEmptyComparisonResult(context.getTargetSchemaLoadTime());
        }

        // Perform comparison and save results
        List<MetadataInstancesCompareDto> compareDtos = performMetadataComparison(
            context, targetMetadataInstances, deductionMap, applyFields
        );

        // Clean up old comparison data if exists
        if (context.getCompareStatus() != null) {
            deleteAll(Query.query(Criteria.where("nodeId").is(context.getNodeId())
                .and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE)));
        }

        // Save new comparison data
        if (CollectionUtils.isNotEmpty(compareDtos)) {
            save(compareDtos, userDetail);

            if (Boolean.TRUE.equals(isSave)) {
                TransformSchemaAsyncService transformSchemaAsyncService =
                        SpringContextHelper.getBean(TransformSchemaAsyncService.class);
                transformSchemaAsyncService.transformSchema(context.getTaskDto().getDag(), userDetail, context.getTaskDto().getId());
            }
        }

        // Update comparison status
        updateComparisonStatus(context.getNodeId(), context.getTargetSchemaLoadTime());

        // Build and return result
        return buildComparisonResult(context, compareDtos);
    }

    /**
     * Creates an empty comparison result when no target metadata instances are found
     */
    protected MetadataInstancesCompareResult createEmptyComparisonResult(Long targetSchemaLoadTime) {
        MetadataInstancesCompareResult result = new MetadataInstancesCompareResult();
        result.setDifferentFieldNumberMap(null);
        if(null != targetSchemaLoadTime){
            result.setTargetSchemaLoadTime(DateUtil.date(targetSchemaLoadTime));
        }
        return result;
    }

    /**
     * Performs the actual metadata comparison
     */
    private List<MetadataInstancesCompareDto> performMetadataComparison(
            ComparisonContext context,
            List<MetadataInstancesDto> targetMetadataInstances,
            Map<String, MetadataInstancesDto> deductionMap,
            Map<String, List<DifferenceField>> applyFields) {

        List<MetadataInstancesCompareDto> compareDtos = new ArrayList<>();

        for (MetadataInstancesDto targetMetadata : targetMetadataInstances) {
            MetadataInstancesDto deductionMetadata = deductionMap.get(context.getCompareIgnoreCase() ? targetMetadata.getName().toLowerCase() : targetMetadata.getName());
            saveMetadataInstancesCompare(
                context.getTaskId(), context.getNodeId(), deductionMetadata,
                targetMetadata, compareDtos, applyFields,context.getCompareIgnoreCase()
            );
        }

        return compareDtos;
    }

    /**
     * Updates the comparison status
     */
    private void updateComparisonStatus(String nodeId, Long targetSchemaLoadTime) {
        MetadataInstancesCompareDto statusDto = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoStatus(nodeId);
        statusDto.setStatus(MetadataInstancesCompareDto.STATUS_DONE);
        statusDto.setLastUpdAt(new Date());
        statusDto.setTargetSchemaLoadTime(DateUtil.date(targetSchemaLoadTime));

        upsert(Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_STATUS)),
               statusDto);
    }

    /**
     * Builds comparison result from context and comparison data
     */
    private MetadataInstancesCompareResult buildComparisonResult(
            ComparisonContext context,
            List<MetadataInstancesCompareDto> compareDtos) {

        MetadataInstancesCompareResult result = new MetadataInstancesCompareResult();
        result.setFinishTime(new Date());
        result.setTargetSchemaLoadTime(DateUtil.date(context.getTargetSchemaLoadTime()));

        getInvalidApplyDtos(compareDtos, context.getApplyDtos(), context.getApplyRules(), result);

        return result;
    }

    /**
     * Builds result for existing comparison that's already done
     */
    private MetadataInstancesCompareResult buildExistingComparisonResult(ComparisonContext context) {
        MetadataInstancesCompareResult result = new MetadataInstancesCompareResult();
        result.setFinishTime(context.getCompareStatus().getLastUpdAt());
        result.setTargetSchemaLoadTime(context.getCompareStatus().getTargetSchemaLoadTime());

        List<MetadataInstancesCompareDto> compareDtos = findAll(
            Query.query(Criteria.where("nodeId").is(context.getNodeId()).and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE))
        );

        getInvalidApplyDtos(compareDtos, context.getApplyDtos(), context.getApplyRules(), result);

        return result;
    }

    @lombok.Builder
    @lombok.Data
    private static class ComparisonContext {
        private String nodeId;
        private String taskId;
        private TaskDto taskDto;
        private String connectionId;
        private MetadataInstancesCompareDto compareStatus;
        private List<MetadataInstancesDto> deductionMetadataInstances;
        private List<String> applyRules;
        private Long targetSchemaLoadTime;
        private List<MetadataInstancesCompareDto> applyDtos;
        private Boolean compareIgnoreCase;
    }

    protected void saveMetadataInstancesCompare(String taskId,String nodeId,MetadataInstancesDto deductionMetadataInstance,MetadataInstancesDto targetMetadataInstance,List<MetadataInstancesCompareDto> compareDtos,Map<String,List<DifferenceField>> applyFields,Boolean compareIgnoreCase) {
        if (null != targetMetadataInstance) {
            Map<String, Field> deductionFieldMap = deductionMetadataInstance.getFields().stream().collect(Collectors.toMap(Field::getFieldName, m -> m));
            List<DifferenceField> applyDifferenceFields =
                    Optional.ofNullable(applyFields)
                            .map(m -> m.get(deductionMetadataInstance.getQualifiedName()))
                            .orElse(Collections.emptyList());
            if(CollectionUtils.isNotEmpty(applyDifferenceFields)){
                applyDifferenceFields.forEach(differenceField -> {
                    differenceField.getType().recoverField(deductionFieldMap.get(differenceField.getColumnName()),deductionMetadataInstance.getFields(),differenceField);
                });
            }

            List<DifferenceField> differenceFieldList = SchemaUtils.compareSchema(deductionMetadataInstance, targetMetadataInstance,compareIgnoreCase);
            compareDtos.add(MetadataInstancesCompareDto.createMetadataInstancesCompareDtoCompare(taskId,nodeId,deductionMetadataInstance.getName(),deductionMetadataInstance.getQualifiedName(),differenceFieldList));
        }
    }

    protected List<MetadataInstancesCompareDto> getAllInvalidApplyDtos(String nodeId,List<String> applyRules,MetadataInstancesCompareResult metadataInstancesCompareResult) {
        List<MetadataInstancesCompareDto> compareDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                .and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE)));
        List<MetadataInstancesCompareDto> applyDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY)));
        List<MetadataInstancesCompareDto> invalidApplyDtos = getInvalidApplyDtos(compareDtos, applyDtos,applyRules,metadataInstancesCompareResult);
        if(CollectionUtils.isNotEmpty(invalidApplyDtos)){
            invalidApplyDtos.forEach(invalidApplyDto -> {
                invalidApplyDto.setDifferenceFieldList(new ArrayList<>());
            });
        }
        return invalidApplyDtos;
    }


    /**
     * 获取无效的配置
     * 如果compareDtos的differenceFieldList中不存在applyDtos differenceFieldList中的DifferenceField columnName，
     * 或者columnName存在但是type不一致或者targetColumnType不一致，则代表配置不生效
     */
    protected List<MetadataInstancesCompareDto> getInvalidApplyDtos(List<MetadataInstancesCompareDto> compareDtos, List<MetadataInstancesCompareDto> applyDtos,List<String> applyRules,MetadataInstancesCompareResult metadataInstancesCompareResult) {
        List<MetadataInstancesCompareDto> invalidApplyDtos = new ArrayList<>();
        // 创建compareDtos的字段映射，按表名和字段名分组
        Map<String, Map<String, DifferenceField>> compareFieldsMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(compareDtos)) {
            for (MetadataInstancesCompareDto compareDto : compareDtos) {
                String tableName = compareDto.getTableName();
                if (CollectionUtils.isNotEmpty(compareDto.getDifferenceFieldList())) {
                    Map<String, DifferenceField> fieldMap = compareDto.getDifferenceFieldList().stream()
                            .collect(Collectors.toMap(DifferenceField::getColumnName, field -> field));
                    fieldMap.values().forEach(differenceField -> {
                        if(CollectionUtils.isNotEmpty(applyRules) && applyRules.contains(differenceField.getType().name())){
                            differenceField.setApplyType(DifferenceField.APPLY_TYPE_AUTO);
                            if(null != metadataInstancesCompareResult){
                                metadataInstancesCompareResult.computeApplyDifferentFieldNumber(differenceField.getType());
                            }
                        }
                        if(null != metadataInstancesCompareResult){
                            metadataInstancesCompareResult.computeDifferentFieldNumber(differenceField.getType());
                            if(differenceField.getIsPrimaryKey() && differenceField.getType() != DifferenceTypeEnum.PrimaryKeyInconsistency){
                                metadataInstancesCompareResult.computeDifferentFieldNumber(DifferenceTypeEnum.PrimaryKeyInconsistency);
                            }
                        }
                    });
                    compareFieldsMap.put(tableName, fieldMap);
                }
            }
        }

        if (CollectionUtils.isEmpty(applyDtos)) {
            return invalidApplyDtos;
        }
        // 检查每个applyDto中的字段是否有效
        for (MetadataInstancesCompareDto applyDto : applyDtos) {
            String tableName = applyDto.getTableName();
            Map<String, DifferenceField> compareFields = compareFieldsMap.get(tableName);


            if (compareFields == null) {
                // 如果compareDtos中没有对应的表，则该applyDto无效
                invalidApplyDtos.add(applyDto);
                continue;
            }

            List<DifferenceField> invalidFields = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(applyDto.getDifferenceFieldList())) {
                for (DifferenceField applyField : applyDto.getDifferenceFieldList()) {
                    String columnName = applyField.getColumnName();
                    DifferenceField compareField = compareFields.get(columnName);
                    if(!applyField.equals(compareField)){
                        invalidFields.add(applyField);
                    }else {
                        if(null != metadataInstancesCompareResult && StringUtils.isBlank(compareField.getApplyType())){
                            metadataInstancesCompareResult.computeApplyDifferentFieldNumber(compareField.getType());
                        }
                        compareField.setApplyType(DifferenceField.APPLY_TYPE_MANUAL);
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(invalidFields)) {
                MetadataInstancesCompareDto invalidApplyDto = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoApply(applyDto.getNodeId(), applyDto.getTableName(), applyDto.getQualifiedName(), invalidFields);
                invalidApplyDtos.add(invalidApplyDto);
            }
        }

        return invalidApplyDtos;
    }

    private void getProcessMetadataInstances(List<MetadataInstancesCompareDto> compareDtos,Boolean apply,UserDetail userDetail,List<String> applyRules){
        Query query = Query.query(Criteria.where("qualified_name").in(compareDtos.stream().map(MetadataInstancesCompareDto::getQualifiedName).collect(Collectors.toList())));
        query.fields().include("qualified_name","name","fields");
        List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findAll(query);
        Map<String,List<DifferenceField>> applyFields = compareDtos.stream().collect(Collectors.toMap(MetadataInstancesCompareDto::getQualifiedName, MetadataInstancesCompareDto::getDifferenceFieldList));
        metadataInstancesDtos.forEach((metadataInstancesDto) -> {
            List<DifferenceField> applyDifferenceFields = applyFields.get(metadataInstancesDto.getQualifiedName());
            Map<String, Field> deductionFieldMap = metadataInstancesDto.getFields().stream().collect(Collectors.toMap(Field::getFieldName, m -> m));
            if(CollectionUtils.isNotEmpty(applyDifferenceFields)){
                applyDifferenceFields.forEach(differenceField -> {
                    if(apply){
                        differenceField.getType().processDifferenceField(deductionFieldMap.get(differenceField.getColumnName()),metadataInstancesDto.getFields(),differenceField);
                    }else{
                        if(CollectionUtils.isNotEmpty(applyRules) && applyRules.contains(differenceField.getType().name())){
                            return;
                        }
                        differenceField.getType().recoverField(deductionFieldMap.get(differenceField.getColumnName()),metadataInstancesDto.getFields(),differenceField);
                    }
                });
            }
        });
        metadataInstancesService.bulkUpsetByWhere(metadataInstancesDtos,userDetail);
    }

    protected List<MetadataInstancesCompareDto> getMetadataInstancesCompareDtosByApplyParams(List<MetadataInstancesApplyParam> metadataInstancesApplyParams,String nodeId) {
        List<MetadataInstancesCompareDto> metadataInstancesCompareDtos = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(metadataInstancesApplyParams)){
            List<String> qualifiedNames = metadataInstancesApplyParams.stream().map(MetadataInstancesApplyParam::getQualifiedName).toList();
            Map<String, List<String>> applyFields = metadataInstancesApplyParams.stream()
                    .collect(Collectors.toMap(
                            MetadataInstancesApplyParam::getQualifiedName,
                            param -> Optional.ofNullable(param.getFieldNames()).orElse(Collections.emptyList())
                    ));
            if(CollectionUtils.isNotEmpty(qualifiedNames)){
                List<MetadataInstancesCompareDto> compareDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                        .and("qualifiedName").in(qualifiedNames).
                        and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE)));
                compareDtos.forEach(compareDto -> {
                    List<DifferenceField> differenceFields;
                    if(applyFields.containsKey(compareDto.getQualifiedName())){
                        if(CollectionUtils.isEmpty(applyFields.get(compareDto.getQualifiedName()))){
                            differenceFields = compareDto.getDifferenceFieldList();
                        }else{
                            differenceFields = compareDto.getDifferenceFieldList().stream()
                                    .filter(differenceField -> applyFields.get(compareDto.getQualifiedName()).contains(differenceField.getColumnName()))
                                    .collect(Collectors.toList());
                        }
                        compareDto.setDifferenceFieldList(differenceFields);
                        metadataInstancesCompareDtos.add(compareDto);
                    }
                });
            }
        }
        return metadataInstancesCompareDtos;
    }



}
