package com.tapdata.tm.metadatainstance.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DifferenceField;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesCompareDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadataInstancesCompare.param.MetadataInstancesApplyParam;
import com.tapdata.tm.metadataInstancesCompare.repository.MetadataInstancesCompareRepository;
import com.tapdata.tm.metadataInstancesCompare.service.MetadataInstancesCompareService;
import com.tapdata.tm.metadatainstance.vo.MetadataInstancesCompareResult;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
        Query compareQuery = Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE));
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
        getProcessMetadataInstances(applyDtos, true, userDetail);
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
        getProcessMetadataInstances(convertedApplyDtos, true, userDetail);
    }

    /**
     * 转换为应用配置DTO
     */
    private List<MetadataInstancesCompareDto> convertToApplyDtos(List<MetadataInstancesCompareDto> compareDtos) {
        return compareDtos.stream()
                .map(dto -> {
                    dto.setId(null);
                    dto.setType(MetadataInstancesCompareDto.TYPE_APPLY);
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
    public void deleteMetadataInstancesCompareApply(List<MetadataInstancesApplyParam> metadataInstancesApplyParams, UserDetail userDetail, Boolean all, String nodeId) {
        if (all) {
            handleDeleteAllApply(nodeId, userDetail);
        } else {
            handleDeletePartialApply(metadataInstancesApplyParams, nodeId, userDetail);
        }
    }

    /**
     * 处理删除所有应用配置
     */
    private void handleDeleteAllApply(String nodeId, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));
        List<MetadataInstancesCompareDto> compareDtos = findAll(query);
        deleteAll(query);
        getProcessMetadataInstances(compareDtos, false, userDetail);
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
                .filter(param -> CollectionUtils.isNotEmpty(param.getFieldNames()))
                .collect(Collectors.toMap(
                        MetadataInstancesApplyParam::getQualifiedName,
                        MetadataInstancesApplyParam::getFieldNames,
                        (existing, replacement) -> existing // 处理重复key的情况
                ));

        // 查询现有数据
        Query query = Query.query(Criteria.where("nodeId").is(nodeId)
                .and("qualifiedName").in(qualifiedNames)
                .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY));

        List<MetadataInstancesCompareDto> existingCompareDtos = findAll(query);
        if (CollectionUtils.isEmpty(existingCompareDtos)) {
            return;
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
            getProcessMetadataInstances(processResult.getRemovedCompareDtos(), false, userDetail);
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

            if (CollectionUtils.isEmpty(fieldsToRemove) || CollectionUtils.isEmpty(compareDto.getDifferenceFieldList())) {
                // 如果没有要删除的字段或者原本就没有字段，跳过处理
                continue;
            }

            // 分离要保留和要删除的字段
            Map<Boolean, List<DifferenceField>> partitionedFields = compareDto.getDifferenceFieldList().stream()
                    .collect(Collectors.partitioningBy(field -> fieldsToRemove.contains(field.getColumnName())));

            List<DifferenceField> fieldsToKeep = partitionedFields.get(false);
            List<DifferenceField> fieldsToDelete = partitionedFields.get(true);

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
    public MetadataInstancesCompareResult getMetadataInstancesCompareResult(String nodeId,String taskId,String tableFilter,int page, int pageSize) {
        Criteria criteria = Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_STATUS);
        Query query = Query.query(criteria);
        MetadataInstancesCompareDto metadataInstancesCompareStatus = findOne(query);
        if (metadataInstancesCompareStatus == null) {return null;}
        MetadataInstancesCompareResult metadataInstancesCompareResult = new MetadataInstancesCompareResult();
        metadataInstancesCompareResult.setStatus(metadataInstancesCompareStatus.getStatus());
        if (metadataInstancesCompareStatus.getStatus().equals(MetadataInstancesCompareDto.STATUS_DONE)) {
            Criteria where = Criteria.where("nodeId").is(nodeId).and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE);
            if (StringUtils.isNotBlank(tableFilter)) {
                Pattern pattern = Pattern.compile(tableFilter, Pattern.CASE_INSENSITIVE);
                where.and("tableName").regex(pattern);
            }
            Query pageQuery = Query.query(where);
            if (pageSize > 0) {
                pageQuery.skip((long) (Math.max(1, page) - 1) * pageSize);
                pageQuery.limit(pageSize);
            }
            List<MetadataInstancesCompareDto> compareDtos = new ArrayList<>(findAll(pageQuery));
            List<String> applyRules = getApplyRules(nodeId,taskId);
            long totals = count(Query.query(where));
            metadataInstancesCompareResult.setCompareDtos(new Page<>(Math.max(totals, compareDtos.size()), compareDtos));
            List<MetadataInstancesCompareDto> applyDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                    .and("type").is(MetadataInstancesCompareDto.TYPE_APPLY)));
            List<MetadataInstancesCompareDto> invalidApplyDtos = getInvalidApplyDtos(compareDtos, applyDtos,applyRules);
            metadataInstancesCompareResult.setInvalidApplyDtos(invalidApplyDtos);
        }

        return metadataInstancesCompareResult;
    }

    @Override
    public List<String> getApplyRules(String nodeId, String taskId) {
        Query query = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId)));
        query.fields().include("dag");
        TaskDto taskDto = taskService.findOne(query);
        if(taskDto == null)return new ArrayList<>();
        DataParentNode targetNode = (DataParentNode)taskDto.getDag().getNode(nodeId);
        Map<String,Boolean> applyCompareRules;
        if(targetNode.getApplyCompareRule()){
            applyCompareRules = targetNode.getApplyCompareRules();
        } else {
            return new ArrayList<>();
        }
        return applyCompareRules.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList();
    }

    /**
     * 获取无效的配置
     * 如果compareDtos的differenceFieldList中不存在applyDtos differenceFieldList中的DifferenceField columnName，
     * 或者columnName存在但是type不一致或者targetColumnType不一致，则代表配置不生效
     */
    protected List<MetadataInstancesCompareDto> getInvalidApplyDtos(List<MetadataInstancesCompareDto> compareDtos, List<MetadataInstancesCompareDto> applyDtos,List<String> applyRules) {
        List<MetadataInstancesCompareDto> invalidApplyDtos = new ArrayList<>();

        if (CollectionUtils.isEmpty(compareDtos)) {
            return invalidApplyDtos;
        }

        // 创建compareDtos的字段映射，按表名和字段名分组
        Map<String, Map<String, DifferenceField>> compareFieldsMap = new HashMap<>();
        for (MetadataInstancesCompareDto compareDto : compareDtos) {
            String tableName = compareDto.getTableName();
            if (CollectionUtils.isNotEmpty(compareDto.getDifferenceFieldList())) {
                Map<String, DifferenceField> fieldMap = compareDto.getDifferenceFieldList().stream()
                        .collect(Collectors.toMap(DifferenceField::getColumnName, field -> field));
                if(CollectionUtils.isNotEmpty(applyRules)){
                    fieldMap.values().forEach(differenceField -> {
                        if(applyRules.contains(differenceField.getType().name())){
                            differenceField.setApplyType(DifferenceField.APPLY_TYPE_AUTO);
                        }
                    });
                }
                compareFieldsMap.put(tableName, fieldMap);
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
                        compareField.setApplyType(DifferenceField.APPLY_TYPE_MANUAL);
                    }
                }
            }

            // 如果有无效字段，创建无效的applyDto
            if (CollectionUtils.isNotEmpty(invalidFields)) {
                MetadataInstancesCompareDto invalidApplyDto = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoApply(applyDto.getNodeId(), applyDto.getTableName(), applyDto.getQualifiedName(), invalidFields);
                invalidApplyDtos.add(invalidApplyDto);
            }
        }

        return invalidApplyDtos;
    }

    private void getProcessMetadataInstances(List<MetadataInstancesCompareDto> compareDtos,Boolean apply,UserDetail userDetail){
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
            Map<String,List<String>> applyFields = metadataInstancesApplyParams.stream()
                    .filter(metadataInstancesApplyParam -> CollectionUtils.isNotEmpty(metadataInstancesApplyParam.getFieldNames()))
                    .collect(Collectors.toMap(MetadataInstancesApplyParam::getQualifiedName, MetadataInstancesApplyParam::getFieldNames));
            if(CollectionUtils.isNotEmpty(qualifiedNames)){
                List<MetadataInstancesCompareDto> compareDtos = findAll(Query.query(Criteria.where("nodeId").is(nodeId)
                        .and("qualifiedName").in(qualifiedNames).
                        and("type").is(MetadataInstancesCompareDto.TYPE_COMPARE)));
                compareDtos.forEach(compareDto -> {
                    if(applyFields.containsKey(compareDto.getQualifiedName())){
                        List<DifferenceField> differenceFields = compareDto.getDifferenceFieldList().stream()
                                .filter(differenceField -> applyFields.get(compareDto.getQualifiedName()).contains(differenceField.getColumnName()))
                                .collect(Collectors.toList());
                        compareDto.setDifferenceFieldList(differenceFields);
                    }
                    metadataInstancesCompareDtos.add(compareDto);
                });
            }
        }
        return metadataInstancesCompareDtos;
    }



}
