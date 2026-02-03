package com.tapdata.tm.metadatadefinition.service;

import com.google.common.collect.Lists;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.service.DiscoveryService;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.repository.MetadataDefinitionRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.task.constant.LdpDirEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class MetadataDefinitionService extends BaseService<MetadataDefinitionDto, MetadataDefinitionEntity, ObjectId, MetadataDefinitionRepository> {

    public static final String ITEM_TYPE = "item_type";

    private static final Map<String, Class<?>> ENTITY_MAP = Map.of(
            "Connections", DataSourceEntity.class,
            "Task", TaskEntity.class,
            "Modules", ModulesEntity.class,
            "Inspect", InspectEntity.class,
            "User", User.class,

            "dataflow", TaskEntity.class,
            "database", DataSourceEntity.class,
            "app", ModulesEntity.class,
            "inspect", InspectEntity.class
    );

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    MetadataInstancesService metadataInstancesService;

    @Autowired
    private DiscoveryService discoveryService;

    @Autowired
    private DataSourceDefinitionService definitionService;

    @Autowired
    private UserService userService;

    @Autowired
    private LdpService ldpService;

    @Autowired
    private SettingsService settingsService;


    public MetadataDefinitionService(@NonNull MetadataDefinitionRepository repository) {
        super(repository, MetadataDefinitionDto.class, MetadataDefinitionEntity.class);
    }

    protected void beforeSave(MetadataDefinitionDto metadataDefinition, UserDetail user) {

    }

    /**
     * 批量修改分类, 每条数据只能有一个分类
     *
     * @param tableName tableName
     * @param batchUpdateParam batchUpdateParam
     * @param userDetail 用户信息
     */
    public List<String> batchUpdateListTags(String tableName, BatchUpdateParam batchUpdateParam, UserDetail userDetail) {
        List<String> idList = batchUpdateParam.getId();
        List<Tag> listTags = batchUpdateParam.getListtags();
        Update update = new Update().set("listtags", listTags);

        // 使用映射表查找对应的实体类
        Class<?> entityClass = ENTITY_MAP.get(tableName);
        if (entityClass != null) {
            mongoTemplate.updateMulti(Query.query(Criteria.where("id").in(idList)), update, entityClass);
        }

        // 更新成功后，需要将模型中的也跟着更新了
        Criteria criteria = Criteria.where("source.id").in(idList).and("metaType").is("database").and("isDeleted").is(false);
        Update classifications = Update.update("classifications", listTags);
        metadataInstancesService.update(new Query(criteria), classifications, userDetail);
        return null;
    }

    /**
     * updates list of tags in a specified table in the database.
     *
     * @param  tableName       the name of the table to update
     * @param  batchUpdateParam the parameters for batch update including id and list of tags
     */
    public List<String> batchPushListTags(String tableName, BatchUpdateParam batchUpdateParam) {
        List<String> idList = batchUpdateParam.getId();
        List<Tag> listTags = batchUpdateParam.getListtags();
        Update update = new Update().addToSet("listtags").each(listTags.toArray());

        Class<?> entityClass = ENTITY_MAP.get(tableName);
        if (entityClass != null) {
            mongoTemplate.updateMulti(Query.query(Criteria.where("id").in(idList)), update, entityClass);
        }

        return idList;
    }


    public void findByItemtypeAndValue(MetadataDefinitionDto metadataDefinitionDto,UserDetail userDetail){
        String value=metadataDefinitionDto.getValue();


        String parentId = metadataDefinitionDto.getParent_id();
        Criteria criteria = Criteria.where("value").is(value);
        if (StringUtils.isBlank(parentId)) {
            criteria.and("parent_id").exists(false);
        } else {
            criteria.and("parent_id").is(parentId);
        }

        if (CollectionUtils.isNotEmpty(metadataDefinitionDto.getItemType())) {
            criteria.and("item_type").in(metadataDefinitionDto.getItemType());
        }

        Query query=Query.query(criteria);
        query.fields().exclude("_id");
        List<MetadataDefinitionDto> metadataDefinitionDtos =findAll(query);
        if (CollectionUtils.isNotEmpty(metadataDefinitionDtos)){
            throw new BizException("Tag.RepeatName");
        }
    }

    /**
     * value 是唯一索引，是不能重复的
     * @param metadataDefinitionDto
     * @param userDetail
     * @return
     */
    public MetadataDefinitionDto save(MetadataDefinitionDto metadataDefinitionDto,UserDetail userDetail){
        MetadataDefinitionDto exsitedOne = null;
        if (metadataDefinitionDto.getId() != null) {
            exsitedOne = findById(metadataDefinitionDto.getId(), userDetail);
            if (null == exsitedOne) {
                throw new BizException("tag.operate.not.allowed");
            }
        }
        List<String> itemType=metadataDefinitionDto.getItemType();
        if (null!=exsitedOne){
            List<String> itemTypeExisted=  exsitedOne.getItemType();
            if (itemTypeExisted == null) {
                itemTypeExisted = new ArrayList<>();
            }
            metadataDefinitionDto.setItemType(itemTypeExisted);
            if (CollectionUtils.isNotEmpty(itemType)) {
                for (String s : itemType) {
                    if (!itemTypeExisted.contains(s)) {
                        itemTypeExisted.add(s);
                    }
                }
            }
        }

        if (StringUtils.isNotBlank(metadataDefinitionDto.getParent_id())) {
            ObjectId parentId = new ObjectId(metadataDefinitionDto.getParent_id());
            MetadataDefinitionDto parent = findById(parentId, userDetail);
            if (null == parent) {
                throw new BizException("tag.operate.not.allowed");
            }
        }
        MetadataDefinitionDto saveValue = super.save(metadataDefinitionDto, userDetail);

        if (exsitedOne != null) {
            if (CollectionUtils.isNotEmpty(saveValue.getItemType()) && StringUtils.isNotBlank(metadataDefinitionDto.getValue())
                    && !metadataDefinitionDto.getValue().equals(exsitedOne.getValue())) {
                Criteria criteria = Criteria.where("listtags")
                        .elemMatch(Criteria.where("id").is(metadataDefinitionDto.getId().toHexString()));
                Update update = Update.update("listtags.$.value", metadataDefinitionDto.getValue());
                Class<?> entityClass = ENTITY_MAP.get(saveValue.getItemType().get(0));

                if (entityClass != null) {
                    mongoTemplate.updateMulti(new Query(criteria), update, entityClass);
                }
            }
        }

        return saveValue;


    }


    public List<MetadataDefinitionDto> findAndParent(List<MetadataDefinitionDto> metadataDefinitionDtos, List<ObjectId> idList) {
        Criteria criteria = Criteria.where("_id").in(idList);
        Query query = new Query(criteria);
        List<MetadataDefinitionDto> all = findAll(query);
        if (metadataDefinitionDtos == null) {
            metadataDefinitionDtos = new ArrayList<>();
        }
        metadataDefinitionDtos.addAll(all);
        List<ObjectId> ids = all.stream().filter(a -> StringUtils.isNotBlank(a.getParent_id())).map(a -> MongoUtils.toObjectId(a.getParent_id())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(ids)) {
            return metadataDefinitionDtos;
        }

        return findAndParent(metadataDefinitionDtos, ids);
    }


    public List<MetadataDefinitionDto> findAndChild(List<ObjectId> idList) {
        Criteria criteria = Criteria.where("_id").in(idList);
        Query query = new Query(criteria);

        List<MetadataDefinitionDto> all = findAll(query);
        return findChild(all, idList);
    }


    public List<MetadataDefinitionDto> findAndChild(List<ObjectId> idList, UserDetail user, String... fields) {
        Criteria criteria = Criteria.where("_id").in(idList);
        Query query = new Query(criteria);
        if (fields != null){
            query.fields().include(fields);
        }

        List<MetadataDefinitionDto> all = findAllDto(query, user);
        return findChild(all, idList, user, fields);
    }

    public List<MetadataDefinitionDto> findAndChild(List<MetadataDefinitionDto> all, MetadataDefinitionDto dto, Map<String, List<MetadataDefinitionDto>> parentMap) {
        if (all == null) {
            all = new ArrayList<>();
            all.add(dto);
        }

        List<MetadataDefinitionDto> metadataDefinitionDtos = parentMap.get(dto.getId().toHexString());
        if (CollectionUtils.isNotEmpty(metadataDefinitionDtos)) {
            all.addAll(metadataDefinitionDtos);
            for (MetadataDefinitionDto metadataDefinitionDto : metadataDefinitionDtos) {
                findAndChild(all, metadataDefinitionDto, parentMap);
            }
        }
        return all;
    }


    private List<MetadataDefinitionDto> findChild(List<MetadataDefinitionDto> metadataDefinitionDtos, List<ObjectId> idList) {
        List<String> collect = idList.stream().map(ObjectId::toHexString).collect(Collectors.toList());
        Criteria criteria = Criteria.where("parent_id").in(collect);
        Query query = new Query(criteria);
        List<MetadataDefinitionDto> all = findAll(query);
        if (CollectionUtils.isEmpty(all)) {
            return metadataDefinitionDtos;
        }
        metadataDefinitionDtos.addAll(all);
        List<ObjectId> ids = all.stream().map(BaseDto::getId).collect(Collectors.toList());
        return findChild(metadataDefinitionDtos, ids);
    }

    private List<MetadataDefinitionDto> findChild(List<MetadataDefinitionDto> metadataDefinitionDtos, List<ObjectId> idList, UserDetail user, String... fields) {
        List<String> collect = idList.stream().map(ObjectId::toHexString).collect(Collectors.toList());
        Criteria criteria = Criteria.where("parent_id").in(collect);
        Query query = new Query(criteria);
        if (fields != null) {
            query.fields().include(fields);
        }
        List<MetadataDefinitionDto> all = findAllDto(query, user);
        if (CollectionUtils.isEmpty(all)) {
            return metadataDefinitionDtos;
        }
        metadataDefinitionDtos.addAll(all);
        List<ObjectId> ids = all.stream().map(BaseDto::getId).collect(Collectors.toList());
        return findChild(metadataDefinitionDtos, ids, user, fields);
    }


    public Page<MetadataDefinitionDto> findAndChildAccount(Filter filter, boolean searchAll, UserDetail user) {
        if (filter == null) {
            filter = new Filter();
        }

        List<MetadataDefinitionEntity> entityList = repository.findAllAndChildAccount(filter, searchAll, user);

        long total = searchAll ?  repository.count(filter.getWhere()) : repository.count(filter.getWhere(), user);

        List<MetadataDefinitionDto> items = convertToDto(entityList, dtoClass);

        return new Page<>(total, items);
    }

    @Override
    public Page<MetadataDefinitionDto> find(Filter filter, UserDetail user) {
        Page<MetadataDefinitionDto> dtoPage;
        Where where = new Where();
        Map<String, String> itemMap = new HashMap<>();
        itemMap.put(ITEM_TYPE, "dataflow");
        where.put("or", com.tapdata.tm.utils.Lists.of(itemMap));
        if (null != filter && where.equals(filter.getWhere())) {
            dtoPage = settingsService.isCloud() ? super.find(filter, user) : super.find(filter);
        } else {
            dtoPage = super.find(filter, user);
        }
        if (filter.getOrder() == null) {
            dtoPage.getItems().sort(Comparator.comparing(MetadataDefinitionDto::getValue));
            dtoPage.getItems().sort(Comparator.comparing(s -> {
                List<String> itemType = s.getItemType();

                return itemType != null && !itemType.contains("default");
            }));
        }

        Set<String> ldpValues = Arrays.stream(LdpDirEnum.values()).map(LdpDirEnum::getValue).collect(Collectors.toSet());
        Set<String> ldpItemTypes = Arrays.stream(LdpDirEnum.values()).map(LdpDirEnum::getItemType).collect(Collectors.toSet());
        Field fields = filter.getFields();
        if (fields != null) {
            Object objCount = fields.get("objCount");
            if (objCount != null && (objCount.equals(true) || (Double) objCount == 1) && CollectionUtils.isNotEmpty(dtoPage.getItems())) {
                discoveryService.addObjCount(dtoPage.getItems(), user);

                if (CollectionUtils.isNotEmpty(dtoPage.getItems())) {
                    List<MetadataDefinitionDto> delItems = new ArrayList<>();
                    List<ObjectId> pdkIdDirectories = new ArrayList<>();
                    Criteria criteriaDefinition = Criteria.where("pdkType").is("pdk")
                            .and("is_deleted").ne(true);
                    Query queryDefinition = new Query(criteriaDefinition);
                    queryDefinition.fields().include("pdkId");
                    List<DataSourceDefinitionDto> dataSourceDefinitionDtos = definitionService.findAllDto(queryDefinition, user);
                    if (CollectionUtils.isNotEmpty(dataSourceDefinitionDtos)) {
                        List<String> pdkIds = dataSourceDefinitionDtos.stream().map(DataSourceDefinitionDto::getPdkId).distinct().collect(Collectors.toList());
                        Criteria in = Criteria.where("item_type").is("default").and("value").in(pdkIds);
                        Query query1 = new Query(in);
                        query1.fields().include("_id");
                        List<MetadataDefinitionDto> pdkDirectories = findAllDto(query1, user);
                        if (CollectionUtils.isNotEmpty(pdkDirectories)) {
                            pdkIdDirectories = pdkDirectories.stream().map(BaseDto::getId).collect(Collectors.toList());
                        }
                    }
                    for (MetadataDefinitionDto item : dtoPage.getItems()) {
                        if ((StringUtils.isNotBlank(item.getLinkId()) || pdkIdDirectories.contains(item.getId())) && item.getObjCount() < 1) {
                            delItems.add(item);
                        }
                    }
                    dtoPage.getItems().removeAll(delItems);
                }
            }
        }

        List<String> userIdList = dtoPage.getItems().stream()
                .filter(d -> d.getItemType() != null && d.getItemType().contains("root"))
                .map(BaseDto::getUserId)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(userIdList)) {
            Map<String, UserDetail> userMap = userService.getUserMapByIdList(userIdList);
            for (MetadataDefinitionDto item : dtoPage.getItems()) {
                if (item.getItemType() != null && item.getItemType().contains("root")) {
                    UserDetail userDetail = userMap.get(item.getUserId());
                    if (userDetail != null) {
                        item.setUserName(StringUtils.isBlank(userDetail.getUsername()) ? userDetail.getEmail() : userDetail.getUsername());
                    }
                }
            }
        }
        return dtoPage;
    }

    public Map<String, String> ldpDirKvs() {
        Criteria criteria = oldLdpCriteria();

        List<MetadataDefinitionDto> ldpDirs = findAll(new Query(criteria));
        Map<String, String> oldLdpMap = null;
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(ldpDirs)) {
            oldLdpMap = ldpDirs.stream().collect(Collectors.toMap(s -> s.getValue(), s -> s.getId().toHexString()));

        }

        return oldLdpMap;
    }


    public void dellOldLdpDirs() {
        Criteria criteria = oldLdpCriteria();
        Query query = new Query(criteria);
        deleteAll(query);
    }

    private Criteria oldLdpCriteria() {
        Criteria criteria = Criteria.where("value").in(Lists.newArrayList(LdpDirEnum.LDP_DIR_SOURCE.getValue(),
                        LdpDirEnum.LDP_DIR_FDM.getValue(), LdpDirEnum.LDP_DIR_MDM.getValue(), LdpDirEnum.LDP_DIR_TARGET.getValue()))
                .and("item_type").in(Lists.newArrayList(LdpDirEnum.LDP_DIR_SOURCE.getItemType(),
                        LdpDirEnum.LDP_DIR_FDM.getItemType(), LdpDirEnum.LDP_DIR_MDM.getItemType(), LdpDirEnum.LDP_DIR_TARGET.getItemType()))
                .and("user_id").exists(false).and("parent_id").exists(false);

        return criteria;
    }

    @Override
    public boolean deleteById(ObjectId id, UserDetail user) {
        MetadataDefinitionDto metadataDefinitionDto = null;
        if (user.isRoot()) {
            metadataDefinitionDto = findById(id);
        } else {
            metadataDefinitionDto = findById(id, user);
        }
        if (metadataDefinitionDto == null) {
            throw new BizException("tag.operate.not.allowed");
        }

        if (CollectionUtils.isNotEmpty(metadataDefinitionDto.getItemType()) && metadataDefinitionDto.getItemType().contains(LdpDirEnum.LDP_DIR_MDM.getItemType())) {


            Tag setTag;
            String parentId = metadataDefinitionDto.getParent_id();
            if (StringUtils.isBlank(parentId)) {
                setTag = ldpService.getMdmTag(user);
            } else {
                Field field = new Field();
                field.put("_id", true);
                field.put("value", true);
                MetadataDefinitionDto parentDefinition = findById(MongoUtils.toObjectId(parentId), field, user);
                if (parentDefinition == null) {
                    setTag = ldpService.getMdmTag(user);
                } else {
                    setTag = new Tag(parentDefinition.getId().toHexString(), parentDefinition.getValue());
                }
            }

            Criteria criteria = Criteria.where("listtags")
                    .elemMatch(Criteria.where("id").is(id.toHexString()));
            Query query = new Query(criteria);
            Update update = Update.update("listtags.$.value", setTag.getValue())
                    .set("listtags.$.id", setTag.getId());

            metadataInstancesService.update(query, update, user);
        }

        return super.deleteById(id, user);


    }
    /**
     * 根据标签优先级对任务进行排序
     */
    public List<TaskDto> orderTaskByTagPriority(List<TaskDto> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }

        Map<String, MetadataDefinitionDto> tagMap = loadTagDefinitionMap();
        if (tagMap.isEmpty()) {
            return new ArrayList<>(tasks);
        }

        try {
            List<TaskDto> sortedTasks = new ArrayList<>(tasks);
            PriorityPathBuilder pathBuilder = new PriorityPathBuilder(tagMap);

            sortedTasks.sort((t1, t2) -> {
                List<Integer> p1 = getTaskPriorityPath(t1, pathBuilder);
                List<Integer> p2 = getTaskPriorityPath(t2, pathBuilder);
                return PriorityPathComparator.compare(p1, p2);
            });

            return sortedTasks;
        } catch (Exception e) {
            log.error("Error ordering tasks by tag priority", e);
            return new ArrayList<>(tasks);
        }
    }

    /**
     * 加载标签定义映射
     */
    private Map<String, MetadataDefinitionDto> loadTagDefinitionMap() {
        try {
            List<MetadataDefinitionDto> taskTags = findAll(
                Query.query(Criteria.where("item_type").is("dataflow"))
            );

            if (CollectionUtils.isEmpty(taskTags)) {
                return Collections.emptyMap();
            }

            return taskTags.stream()
                .filter(Objects::nonNull)
                .filter(tag -> tag.getId() != null)
                .collect(Collectors.toMap(
                    tag -> tag.getId().toHexString(),
                    Function.identity(),
                    (existing, replacement) -> existing
                ));
        } catch (Exception e) {
            log.error("Error loading tag definitions", e);
            return Collections.emptyMap();
        }
    }

    /**
     * 获取任务的优先级路径（简化版本）
     */
    private List<Integer> getTaskPriorityPath(TaskDto task, PriorityPathBuilder pathBuilder) {
        if (task == null || CollectionUtils.isEmpty(task.getListtags())) {
            return Collections.emptyList();
        }

        // 找到最高优先级的标签
        Tag highestPriorityTag = task.getListtags().stream()
            .filter(Objects::nonNull)
            .filter(tag -> StringUtils.isNotBlank(tag.getId()))
            .min((t1, t2) -> {
                List<Integer> p1 = pathBuilder.buildPath(t1.getId());
                List<Integer> p2 = pathBuilder.buildPath(t2.getId());
                return PriorityPathComparator.compare(p1, p2);
            })
            .orElse(null);

        return highestPriorityTag != null ?
            pathBuilder.buildPath(highestPriorityTag.getId()) :
            Collections.emptyList();
    }

    private static class PriorityPathBuilder {
        private final Map<String, MetadataDefinitionDto> tagMap;
        private final Map<String, List<Integer>> pathCache;

        public PriorityPathBuilder(Map<String, MetadataDefinitionDto> tagMap) {
            this.tagMap = tagMap;
            this.pathCache = new ConcurrentHashMap<>();
        }

        public List<Integer> buildPath(String tagId) {
            if (StringUtils.isBlank(tagId)) {
                return Collections.emptyList();
            }
            return pathCache.computeIfAbsent(tagId, this::buildPathInternal);
        }

        private List<Integer> buildPathInternal(String tagId) {
            if (tagMap.isEmpty()) {
                return Collections.emptyList();
            }

            Deque<Integer> pathStack = new ArrayDeque<>();
            MetadataDefinitionDto current = tagMap.get(tagId);
            Set<String> visited = new HashSet<>();

            while (current != null && !visited.contains(current.getId().toHexString())) {
                visited.add(current.getId().toHexString());
                Integer priority = current.getPriority();
                pathStack.push(priority != null ? priority : Integer.MAX_VALUE);

                if (StringUtils.isBlank(current.getParent_id())) {
                    break;
                }
                current = tagMap.get(current.getParent_id());
            }

            return new ArrayList<>(pathStack);
        }
    }

    private static class PriorityPathComparator {
        public static int compare(List<Integer> p1, List<Integer> p2) {
            int len = Math.min(p1.size(), p2.size());

            // 逐级比较优先级（降序）
            for (int i = 0; i < len; i++) {
                int cmp = Integer.compare(p1.get(i), p2.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }

            // 如果前面都一样，路径长的排后面
            return Integer.compare(p1.size(), p2.size());
        }
    }

    /**
     * 批量导入标签定义
     * 如果标签名已存在则更新，否则新建
     *
     * @param tags 标签定义列表
     * @param user 用户信息
     * @return 旧标签ID到新标签ID的映射
     */
    public Map<String, String> batchImport(List<MetadataDefinitionDto> tags, UserDetail user) {
        Map<String, String> tagIdMap = new HashMap<>();
        if (CollectionUtils.isEmpty(tags)) {
            return tagIdMap;
        }
        List<MetadataDefinitionDto> sortTags = tags.stream()
                .sorted(Comparator.comparing((MetadataDefinitionDto dto) -> org.apache.commons.lang3.StringUtils.isNotBlank(dto.getParent_id()))).toList();
        for (MetadataDefinitionDto tag : sortTags) {
            String oldId = tag.getId() != null ? tag.getId().toHexString() : null;
            String tagValue = tag.getValue();

            if (StringUtils.isBlank(tagValue)) {
                continue;
            }

            // 查找是否已存在同名标签
            Criteria criteria = Criteria.where("value").is(tagValue).and("item_type").is(tag.getItemType());
            Query query = new Query(criteria);
            MetadataDefinitionDto existing = findOne(query);

            if (existing != null) {
                // 标签已存在，更新并记录映射
                if (oldId != null) {
                    tagIdMap.put(oldId, existing.getId().toHexString());
                }
                tag.setId(null);
                if(StringUtils.isNotBlank(tag.getParent_id())) {
                    tag.setParent_id(tagIdMap.get(tag.getParent_id()));
                }
                update(Query.query(Criteria.where("_id").is(existing.getId())),tag);
            } else {
                // 标签不存在，新建
                tag.setCreateUser(null);
                tag.setCustomId(null);
                tag.setLastUpdBy(null);
                tag.setUserId(null);
                tag.setId(null);
                if(StringUtils.isNotBlank(tag.getParent_id())) {
                    tag.setParent_id(tagIdMap.get(tag.getParent_id()));
                }
                MetadataDefinitionDto savedTag = super.save(tag,user);
                tagIdMap.put(oldId, savedTag.getId().toHexString());
            }
        }

        return tagIdMap;
    }

}