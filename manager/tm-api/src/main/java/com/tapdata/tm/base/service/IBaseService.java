package com.tapdata.tm.base.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.base.dto.UpdateDto;
import com.tapdata.tm.config.security.UserDetail;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.util.List;

public interface IBaseService<Dto extends BaseDto, Entity extends BaseEntity, ID extends Serializable, Repository extends BaseRepository<Entity, ID>> {
    Page<Dto> find(Filter filter, UserDetail userDetail);

    Page<Dto> find(Filter filter, String excludeField, UserDetail userDetail);

    Page<Dto> find(Filter filter);

    List<Dto> findAll(Filter filter);

    List<Entity> findAllEntity(Query query);

    List<Entity> findAll(Query query, UserDetail userDetail);

    List<Dto> findAllDto(Query query, UserDetail userDetail);

    List<Dto> findAll(Query query);

    List<Dto> findAllNotDeleted(Query query);

    List<Entity> findAll(UserDetail userDetail);

    <T extends BaseDto> Dto save(Dto dto, UserDetail userDetail);

    <T extends BaseDto> List<Dto> save(List<Dto> dtoList, UserDetail userDetail);

    boolean deleteById(ID id, UserDetail userDetail);

    boolean deleteById(ID id);

    UpdateResult deleteLogicsById(String id);

    Dto findById(ID id, UserDetail userDetail);

    Dto findById(ID id, Field field, UserDetail userDetail);

    Dto findById(ID id);

    Dto findById(ID id, Field field);

    Dto findOne(Query query, UserDetail userDetail);

    Dto findOne(Query query);

    Dto findOne(Query query, String excludeField);

    Dto findOne(Filter filter, UserDetail userDetail);

    List<Dto> convertToDto(List<Entity> entityList, Class<Dto> dtoClass, String... ignoreProperties);

    <T extends BaseDto> T convertToDto(Entity entity, Class<T> dtoClass, String... ignoreProperties);

    <T extends BaseDto> List<Entity> convertToEntity(Class<Entity> entityClass, List<T> dtoList, String... ignoreProperties);

    <T extends BaseDto> Entity convertToEntity(Class<Entity> entityClass, T dto, String... ignoreProperties);

    UpdateResult updateById(ID id, Update update, UserDetail userDetail);

    UpdateResult updateByIdNotChangeLast(ID id, Update update, UserDetail userDetail);

    UpdateResult updateById(String id, Update update, UserDetail userDetail);

    long updateByWhere(Where where, UpdateDto<Dto> dto, UserDetail userDetail);

    long updateByWhere(Where where, Document doc, UserDetail userDetail);

    long updateByWhere(Where where, Dto dto, UserDetail userDetail);

    long updateByWhere(Query query, Dto dto, UserDetail userDetail);

    <T extends BaseDto> long upsert(Query query, T dto, UserDetail userDetail);

    <T extends BaseDto> long upsert(Query query, T dto);

    Dto upsertByWhere(Where where, Dto dto, UserDetail userDetail);

    List<Dto> findAll(Where where, UserDetail userDetail);

    List<Dto> findAll(Where where);

    UpdateResult update(Query query, Update update, UserDetail userDetail);

    UpdateResult update(Query query, Dto dto);

    UpdateResult update(Query query, Update update);

    UpdateResult updateMany(Query query, Update update);

    Entity findAndModify(Query query, Update update, UserDetail userDetail);

    void deleteAll(Query query, UserDetail userDetail);

    long deleteAll(Query query);

    long count(Where where, UserDetail userDetail);

    long count(Query query);

    long count(Query query, UserDetail userDetail);

    Dto replaceById(ID id, Dto dto, UserDetail userDetail);

    Dto replaceOrInsert(Dto dto, UserDetail userDetail);
}
