package com.tapdata.tm.base.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Maps;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.base.dto.UpdateDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.ThrowableUtils;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 4:29 下午
 * @description
 */
@RequiredArgsConstructor
@Slf4j
public abstract class BaseService<Dto extends BaseDto, Entity extends BaseEntity, ID extends Serializable, Repository extends BaseRepository<Entity, ID>> {

    @NonNull
    protected Repository repository;
    @NonNull
    protected Class<Dto> dtoClass;
    @NonNull
    protected Class<Entity> entityClass;


    protected boolean isAgentReq() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();
        String userAgent = request.getHeader("user-agent");
        return StringUtils.isNotBlank(userAgent) && (userAgent.contains("Java") || userAgent.contains("Node") || userAgent.contains("FlowEngine"));
    }

    /**
     * Paging query
     *
     * @param filter optional, page query parameters
     * @return the Page of current page, include page data and total size.
     */
    public Page<Dto> find(Filter filter, UserDetail userDetail) {

        if (filter == null) {
            filter = new Filter();
        }

        List<Entity> entityList = repository.findAll(filter, userDetail);

        long total = repository.count(filter.getWhere(), userDetail);

        List<Dto> items = convertToDto(entityList, dtoClass, "password");

        return new Page<>(total, items);
    }


    /**
     * Paging query
     *
     * @param filter optional, page query parameters
     * @return the Page of current page, include page data and total size.
     */
    public Page<Dto> find(Filter filter, String excludeField, UserDetail userDetail) {

        if (filter == null)
            filter = new Filter();

        List<Entity> entityList = repository.findAll(filter,excludeField, userDetail);

        long total = repository.count(filter.getWhere(), userDetail);

        List<Dto> items = convertToDto(entityList, dtoClass, "password");

        return new Page<>(total, items);
    }

    /**
     * Paging query
     *
     * @param filter optional, page query parameters
     * @return the Page of current page, include page data and total size.
     */
    public Page<Dto> find(Filter filter) {

        if (filter == null)
            filter = new Filter();

        List<Entity> entityList = repository.findAll(filter);

        long total = repository.count(filter.getWhere());

        List<Dto> items = convertToDto(entityList, dtoClass, "password");

        return new Page<>(total, items);
    }

    /**
     * 只返回没有被逻辑删除的
     *
     * @param filter
     * @return
     */
    public List<Dto> findAll(Filter filter) {
        JSONObject jsonObject = JSONUtil.parseObj(filter);
        JSONObject whereJsonObject = jsonObject.getJSONObject("where");

        Sort sort = Sort.by("createAt").descending();
        JSONObject orderJsonObject = jsonObject.getJSONObject("order");
        if (null != orderJsonObject) {
            List<java.lang.reflect.Field> fieldLis = Arrays.asList(dtoClass.getDeclaredFields());

            for (java.lang.reflect.Field field : fieldLis) {
                String filedName = field.getName();
                String orderByString = orderJsonObject.toString();
                if (orderByString.contains(filedName) && orderByString.contains("DESC")) {
                    sort = Sort.by(filedName).descending();
                } else if (orderByString.contains(filedName) && orderByString.contains("ASC")) {
                    sort = Sort.by(filedName).ascending();
                }
            }
        }

        List entityList = repository.findAll(whereJsonObject);
        List<Dto> dtoList = convertToDto(entityList, dtoClass);

        return dtoList;
    }

    public List<Entity> findAllEntity(Query query) {
        return repository.findAll(query);
    }

    public List<Entity> findAll(Query query, UserDetail userDetail) {
        return repository.findAll(query, userDetail);
    }

    public List<Dto> findAllDto(Query query, UserDetail userDetail) {
        return repository.findAll(query, userDetail).stream().map( entity -> convertToDto(entity, dtoClass)).collect(Collectors.toList());
    }


    public List<Dto> findAll(Query query) {
        return repository.findAll(query).stream().map(entity -> convertToDto(entity, dtoClass)).collect(Collectors.toList());
    }

    /**
     * 查询所有没有被逻辑删除的
     * @param query
     * @return
     */
    public List<Dto> findAllNotDeleted(Query query) {
        query.addCriteria(Criteria.where("is_deleted").ne(true));
        return repository.findAll(query).stream().map(entity -> convertToDto(entity, dtoClass)).collect(Collectors.toList());
    }


    public List<Entity> findAll(UserDetail userDetail) {

        return repository.findAll(userDetail);
    }

    /**
     * Save the object to the collection for the entity type of the object to save. This will perform an insert if the
     * object is not already present, that is an 'upsert'.
     *
     * @param dto required
     * @return Data after persistence
     */
    public <T extends BaseDto> Dto save(Dto dto, UserDetail userDetail) {

        Assert.notNull(dto, "Dto must not be null!");

        beforeSave(dto, userDetail);

        Entity entity = convertToEntity(entityClass, dto);

        entity = repository.save(entity, userDetail);

        BeanUtils.copyProperties(entity, dto);

        return dto;
    }

    public <T extends BaseDto> List<Dto> save(List<Dto> dtoList, UserDetail userDetail) {
        Assert.notNull(dtoList, "Dto must not be null!");

        List<Entity> entityList = new ArrayList<>();
        for (Dto dto : dtoList) {
            beforeSave(dto, userDetail);

            Entity entity = convertToEntity(entityClass, dto);
            entityList.add(entity);
        }

        entityList = repository.saveAll(entityList, userDetail);

        dtoList = convertToDto(entityList, dtoClass);

        return dtoList;
    }

    protected abstract void beforeSave(Dto dto, UserDetail userDetail);

    public boolean deleteById(ID id, UserDetail userDetail) {

        Assert.notNull(id, "Id must not be null!");
        return repository.deleteById(id, userDetail);
    }

    public boolean deleteById(ID id) {

        Assert.notNull(id, "Id must not be null!");
        return repository.deleteById(id);
    }

    public UpdateResult deleteLogicsById(String id) {
        Assert.notNull(id, "Id must not be null!");
        Update update = Update.update("is_deleted", true);
        Query query = new Query();
        query.addCriteria(Criteria.where("id").is(id));
        UpdateResult updateResult = repository.getMongoOperations().updateMulti(query, update, entityClass);
        return updateResult;
    }


    /**
     * find model by id
     *
     * @param id
     * @param userDetail
     * @return
     */
    public Dto findById(ID id, UserDetail userDetail) {
        Assert.notNull(id, "Id must not be null!");
        Optional<Entity> entity = repository.findById(id, userDetail);
        return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
    }

//	public Dto findById(String id) {
//		Assert.notNull(id, "Id must not be null!");
//		Query query=new Query().addCriteria(Criteria.where("_id").is(id));
//		Optional<Entity> entity = repository.findOne(query);
//		return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
//	}


    /**
     * find model by id
     *
     * @param id
     * @param userDetail
     * @return
     */
    public Dto findById(ID id, Field field, UserDetail userDetail) {
        Assert.notNull(id, "Id must not be null!");
        Optional<Entity> entity;
        if (field == null) {
            entity = repository.findById(id, userDetail);
        } else {
            entity = repository.findById(id, field, userDetail);
        }

        return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
    }

    /**
     * find model by id
     *
     * @param id
     * @return
     */
    public Dto findById(ID id) {
        return findById(id, new Field());
    }


    public Dto findById(ID id, Field field) {
        Assert.notNull(id, "Id must not be null!");
        Optional<Entity> entity = repository.findById(id, field);
        return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
    }

    /**
     * find one model
     *
     * @param query
     * @param userDetail
     * @return
     */
    public Dto findOne(Query query, UserDetail userDetail) {
        return repository.findOne(query, userDetail).map(entity -> convertToDto(entity, dtoClass)).orElse(null);
    }

    /**
     * find one model
     *
     * @param query
     * @return
     */
    public Dto findOne(Query query) {
        return repository.findOne(query).map(entity -> convertToDto(entity, dtoClass)).orElse(null);
    }
    /**
     * find one model
     *
     * @param query
     * @return
     */
    public Dto findOne(Query query,String excludeField) {
        query.fields().exclude(excludeField);
        return repository.findOne(query).map(entity -> convertToDto(entity, dtoClass)).orElse(null);
    }

    /**
     * find one model
     *
     * @param filter
     * @param userDetail
     * @return
     */
    public Dto findOne(Filter filter, UserDetail userDetail) {
        Query query = repository.filterToQuery(filter);
        return findOne(query, userDetail);
    }

    /**
     * Convert DB Entity to Dto
     *
     * @param entityList       required, the record List of entity.
     * @param dtoClass         required, the Class of Dto.
     * @param ignoreProperties optional, fields that do not need to be processed during conversion.
     * @return the List of converted.
     */
    public List<Dto> convertToDto(List<Entity> entityList, Class<Dto> dtoClass, String... ignoreProperties) {
        if (entityList == null)
            return null;

        return entityList.stream().map(entity -> convertToDto(entity, dtoClass, ignoreProperties))
                .collect(Collectors.toList());
    }

    /**
     * Convert DB Entity to Dto
     *
     * @param entity           required, the record of Entity.
     * @param dtoClass         required, the Class of Dto.
     * @param ignoreProperties optional, fields that do not need to be processed during conversion.
     * @return the Dto of converted.
     */
    public <T extends BaseDto> T convertToDto(Entity entity, Class<T> dtoClass, String... ignoreProperties) {
        if (dtoClass == null || entity == null)
            return null;

        try {
            T target = dtoClass.getDeclaredConstructor().newInstance();

            BeanUtils.copyProperties(entity, target, ignoreProperties);

            return target;
        } catch (Exception e) {
            log.error("Convert dto " + dtoClass + " failed. {}", ThrowableUtils.getStackTraceByPn(e));
        }
        return null;
    }

    /**
     * Convert Dto to DB Entity.
     *
     * @param entityClass      required, the Class of entity.
     * @param dtoList          required, the record list of dto.
     * @param ignoreProperties optional, fields that do not need to be processed during conversion.
     * @return the List of converted.
     */
    public <T extends BaseDto> List<Entity> convertToEntity(Class<Entity> entityClass, List<T> dtoList, String... ignoreProperties) {
        if (dtoList == null)
            return null;

        return dtoList.stream().map(dto -> convertToEntity(entityClass, dto, ignoreProperties))
                .collect(Collectors.toList());
    }

    /**
     * Convert Dto to DB Entity
     *
     * @param entityClass      required, the Class of entity
     * @param dto              required, the record of dto.
     * @param ignoreProperties optional, fields that do not need to be processed during conversion.
     * @return the List of converted.
     */
    public <T extends BaseDto> Entity convertToEntity(Class<Entity> entityClass, T dto, String... ignoreProperties) {

        if (entityClass == null || dto == null)
            return null;

        try {
            Entity entity = entityClass.getDeclaredConstructor().newInstance();

            BeanUtils.copyProperties(dto, entity, ignoreProperties);

            return entity;
        } catch (Exception e) {
            log.error("Convert entity " + entityClass + " failed. {}", ThrowableUtils.getStackTraceByPn(e));
        }
        return null;
    }

    public UpdateResult updateById(ID id, Update update, UserDetail userDetail) {
        Assert.notNull(id, "Id must not be null!");

        return repository.updateFirst(new Query(Criteria.where("_id").is(id)), update, userDetail);
    }

    public UpdateResult updateById(String id, Update update, UserDetail userDetail) {
        Assert.notNull(id, "Id must not be null!");

        return repository.updateFirst(new Query(Criteria.where("_id").is(id)), update, userDetail);
    }


    public long updateByWhere(Where where, UpdateDto<Dto> dto, UserDetail userDetail) {
        Filter filter = new Filter(where);
        filter.setLimit(0);
        filter.setSkip(0);
        Query query = repository.filterToQuery(filter);
        Entity set = convertToEntity(entityClass, dto.getSet());
        Entity setOnInsert = convertToEntity(entityClass, dto.getSetOnInsert());
        return repository.updateByWhere(query, set, setOnInsert, dto.getUnset(), userDetail).getModifiedCount();
    }

    public long updateByWhere(Where where, Document doc, UserDetail userDetail) {
        Filter filter = new Filter(where);
        filter.setLimit(0);
        filter.setSkip(0);
        Query query = repository.filterToQuery(filter);

        return repository.update(query, Update.fromDocument(doc), userDetail).getModifiedCount();
    }

    public long updateByWhere(Where where, Dto dto, UserDetail userDetail) {

        beforeSave(dto, userDetail);
        Filter filter = new Filter(where);
        filter.setLimit(0);
        filter.setSkip(0);
        Query query = repository.filterToQuery(filter);
        Entity entity = convertToEntity(entityClass, dto);
        UpdateResult updateResult = repository.updateByWhere(query, entity, userDetail);
        return updateResult.getModifiedCount();
    }

    public long updateByWhere(Query query, Dto dto, UserDetail userDetail) {
        beforeSave(dto, userDetail);
        Entity entity = convertToEntity(entityClass, dto);
        UpdateResult updateResult = repository.updateByWhere(query, entity, userDetail);
        return updateResult.getModifiedCount();
    }

    public <T extends BaseDto> long upsert(Query query, T dto, UserDetail userDetail) {

        long count = repository.upsert(query, convertToEntity(entityClass, dto), userDetail);

        return count;
    }

    public <T extends BaseDto> long upsert(Query query, T dto) {

        return repository.upsert(query, convertToEntity(entityClass, dto));
    }

    public Dto upsertByWhere(Where where, Dto dto, UserDetail userDetail) {

        beforeSave(dto, userDetail);
        Filter filter = new Filter(where);
        filter.setLimit(0);
        filter.setSkip(0);
        Query query = repository.filterToQuery(filter);
        repository.upsert(query, convertToEntity(entityClass, dto), userDetail);
        Optional<Entity> optional = repository.findOne(where, userDetail);

        return optional.map(entity -> convertToDto(entity, dtoClass)).orElse(null);
    }

    public List<Dto> findAll(Where where, UserDetail userDetail) {
        List<Entity> entities = repository.findAll(where, userDetail);
        return convertToDto(entities, dtoClass, "password");
    }

    public List<Dto> findAll(Where where) {
        List<Entity> entities = repository.findAll(where);
        return convertToDto(entities, dtoClass, "password");
    }


    public UpdateResult update(Query query, Update update, UserDetail userDetail) {
        return repository.update(query, update, userDetail);
    }

    public UpdateResult update(Query query, Dto dto) {
        return repository.update(query, convertToEntity(entityClass, dto));
    }

    public UpdateResult update(Query query, Update update) {
        return repository.update(query, update);
    }

    public Entity findAndModify(Query query, Update update, UserDetail userDetail) {

        return repository.findAndModify(query, update, userDetail);
    }

    public void deleteAll(Query query, UserDetail userDetail) {
        repository.deleteAll(query, userDetail);
    }

    public long deleteAll(Query query) {
        return repository.deleteAll(query);
    }

    public long count(Where where, UserDetail userDetail) {

        return repository.count(where, userDetail);
    }

    public long count(Query query) {
        return repository.count(query);
    }

    public long count(Query query, UserDetail userDetail) {
        return repository.count(query, userDetail);
    }


    public Dto replaceById(ID id, Dto dto, UserDetail userDetail) {
        Entity entity = repository.replaceById(new Query(Criteria.where("_id").is(id)), convertToEntity(entityClass, dto), userDetail);
        return convertToDto(entity, dtoClass);
    }

    public Dto replaceOrInsert(Dto dto, UserDetail userDetail) {
        Entity entity = repository.replaceOrInsert(new Query(Criteria.where("_id").is(dto.getId())), convertToEntity(entityClass, dto), userDetail);
        return convertToDto(entity, dtoClass);
    }
}
