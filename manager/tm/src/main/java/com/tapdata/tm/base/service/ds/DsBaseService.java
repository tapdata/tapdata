//package com.tapdata.tm.base.service.ds;
//
//import com.mongodb.client.result.UpdateResult;
//import com.tapdata.tm.base.dto.Field;
//import com.tapdata.tm.base.dto.Filter;
//import com.tapdata.tm.base.dto.Page;
//import com.tapdata.tm.base.dto.Where;
//import com.tapdata.tm.base.dto.ds.DsBaseDto;
//import com.tapdata.tm.base.dto.ds.UpdateDto;
//import com.tapdata.tm.base.entity.ds.BaseEntity;
//import com.tapdata.tm.base.reporitory.ds.DsBaseRepository;
//import com.tapdata.tm.config.security.UserDetail;
//import com.tapdata.tm.ds.dto.DataSourceConnectionDto;
//import lombok.NonNull;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.BeanUtils;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.core.query.Update;
//import org.springframework.util.Assert;
//
//import java.io.Serializable;
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * @author lg<lirufei0808 @ gmail.com>
// * @date 2020/9/11 4:29 下午
// * @description
// */
//@RequiredArgsConstructor
//@Slf4j
//public abstract class DsBaseService<Dto extends DsBaseDto, Entity extends BaseEntity, ID extends Serializable, Repository extends DsBaseRepository<Entity, ID>> {
//
//	@NonNull
//	protected Repository repository;
//	@NonNull
//	protected Class<Dto> dtoClass;
//	@NonNull
//	protected Class<Entity> entityClass;
//
//	/**
//	 * Paging query
//	 * @param filter 		optional, page query parameters
//	 * @return the Page of current page, include page data and total size.
//	 */
//	public Page<Dto> find(Filter filter, UserDetail userDetail) {
//
//		if (filter == null)
//			filter = new Filter();
//
//		List<Entity> entityList = repository.findAll(filter, userDetail);
//
//		long total = repository.count(filter.getWhere(), userDetail);
//
//		List<Dto> items = convertToDto(entityList, dtoClass, "password");
//
//		return new Page<>(total, items);
//	}
//
//	/**
//	 * Paging query
//	 * @param filter 		optional, page query parameters
//	 * @return the Page of current page, include page data and total size.
//	 */
//	public Page<Dto> find(Filter filter) {
//
//		if (filter == null)
//			filter = new Filter();
//
//		List<Entity> entityList = repository.findAll(filter);
//
//		long total = repository.count(filter.getWhere());
//
//		List<Dto> items = convertToDto(entityList, dtoClass, "password");
//
//		return new Page<>(total, items);
//	}
//
//	public List<Entity> findAll(Query query, UserDetail userDetail) {
//		return repository.findAll(query, userDetail);
//	}
//
//	public List<Dto> findAllDto(Query query, UserDetail userDetail) {
//		List<Entity> all = repository.findAll(query, userDetail);
//		return convertToDto(all, dtoClass);
//	}
//
//
//
//	public List<Dto> findAll(Query query) {
//		return repository.findAll(query).stream().map(entity -> convertToDto(entity, dtoClass)).collect(Collectors.toList());
//	}
//	public List<Entity> findAll(UserDetail userDetail) {
//
//		return repository.findAll(userDetail);
//	}
//
//	/**
//	 * Save the object to the collection for the entity type of the object to save. This will perform an insert if the
//	 * object is not already present, that is an 'upsert'.
//	 * @param dto required
//	 * @return Data after persistence
//	 */
//	public <T extends DsBaseDto> Dto save(Dto dto, UserDetail userDetail) {
//
//		Assert.notNull(dto, "Dto must not be null!");
//
//		beforeSave(dto, userDetail);
//
//		Entity entity = convertToEntity(entityClass, dto);
//
//		entity = repository.save(entity, userDetail);
//
//		BeanUtils.copyProperties(entity, dto);
//
//		return dto;
//	}
//
//	public <T extends DsBaseDto> List<Dto> save(List<Dto> dtoList, UserDetail userDetail) {
//		Assert.notNull(dtoList, "Dto must not be null!");
//
//		List<Entity> entityList = new ArrayList<>();
//		for (Dto dto : dtoList) {
//			beforeSave(dto, userDetail);
//
//			Entity entity = convertToEntity(entityClass, dto);
//			entityList.add(entity);
//		}
//
//		entityList = repository.saveAll(entityList, userDetail);
//
//		dtoList = convertToDto(entityList, dtoClass);
//
//		return dtoList;
//	}
//
//	protected abstract void beforeSave(Dto dto, UserDetail userDetail);
//
//	public boolean deleteById(ID id, UserDetail userDetail) {
//
//		Assert.notNull(id, "Id must not be null!");
//		return repository.deleteById(id, userDetail);
//	}
//
//	/**
//	 * find model by id
//	 * @param id
//	 * @param userDetail
//	 * @return
//	 */
//	public Dto findById(ID id, UserDetail userDetail) {
//		Assert.notNull(id, "Id must not be null!");
//		Optional<Entity> entity = repository.findById(id, userDetail);
//		return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
//	}
//
////	public Dto findById(String id) {
////		Assert.notNull(id, "Id must not be null!");
////		Query query=new Query().addCriteria(Criteria.where("_id").is(id));
////		Optional<Entity> entity = repository.findOne(query);
////		return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
////	}
//
//
//	/**
//	 * find model by id
//	 * @param id
//	 * @param userDetail
//	 * @return
//	 */
//	public Dto findById(ID id, Field field, UserDetail userDetail) {
//		Assert.notNull(id, "Id must not be null!");
//		Optional<Entity> entity;
//		if (field == null) {
//			entity = repository.findById(id, userDetail);
//		} else {
//			entity = repository.findById(id, field, userDetail);
//		}
//
//		return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
//	}
//
//	/**
//	 * find model by id
//	 * @param id
//	 * @return
//	 */
//	public Dto findById(ID id) {
//		return findById(id, new Field());
//	}
//	public Dto findById(ID id, Field field) {
//		Assert.notNull(id, "Id must not be null!");
//		Optional<Entity> entity = repository.findById(id, field);
//		return entity.map(value -> convertToDto(value, dtoClass)).orElse(null);
//	}
//
//	/**
//	 * find one model
//	 * @param query
//	 * @param userDetail
//	 * @return
//	 */
//	public Dto findOne(Query query, UserDetail userDetail) {
//		return repository.findOne(query, userDetail).map( entity -> convertToDto(entity, dtoClass)).orElse(null);
//	}
//
//	/**
//	 * find one model
//	 * @param query
//	 * @return
//	 */
//	public Dto findOne(Query query) {
//		return repository.findOne(query).map( entity -> convertToDto(entity, dtoClass)).orElse(null);
//	}
//
//	/**
//	 * find one model
//	 * @param filter
//	 * @param userDetail
//	 * @return
//	 */
//	public Dto findOne(Filter filter, UserDetail userDetail) {
//		Query query = repository.filterToQuery(filter);
//		return findOne(query, userDetail);
//	}
//
//	/**
//	 * Convert DB Entity to Dto
//	 *
//	 * @param entityList			required, the record List of entity.
//	 * @param dtoClass				required, the Class of Dto.
//	 * @param ignoreProperties		optional, fields that do not need to be processed during conversion.
//	 * @return the List of converted.
//	 */
//	public List<Dto> convertToDto(List<Entity> entityList, Class<Dto> dtoClass, String... ignoreProperties) {
//		if (entityList == null)
//			return null;
//
//		return entityList.stream().map(entity -> convertToDto(entity,dtoClass, ignoreProperties))
//				.collect(Collectors.toList());
//	}
//
//	/**
//	 * Convert DB Entity to Dto
//	 * @param entity				required, the record of Entity.
//	 * @param dtoClass				required, the Class of Dto.
//	 * @param ignoreProperties		optional, fields that do not need to be processed during conversion.
//	 * @return the Dto of converted.
//	 */
//	public <T extends DsBaseDto> T convertToDto(Entity entity, Class<T> dtoClass, String... ignoreProperties) {
//		if (dtoClass == null || entity == null)
//			return null;
//
//		try {
//			T target = dtoClass.getDeclaredConstructor().newInstance();
//
//			BeanUtils.copyProperties(entity, target, ignoreProperties);
//
//			return target;
//		} catch (Exception e) {
//			log.error("Convert dto " + dtoClass + " failed.", e);
//		}
//		return null;
//	}
//
//	/**
//	 * Convert Dto to DB Entity.
//	 *
//	 * @param entityClass		required, the Class of entity.
//	 * @param dtoList			required, the record list of dto.
//	 * @param ignoreProperties	optional, fields that do not need to be processed during conversion.
//	 * @return the List of converted.
//	 */
//	public <T extends DsBaseDto> List<Entity> convertToEntity(Class<Entity> entityClass, List<T> dtoList, String... ignoreProperties) {
//		if (dtoList == null)
//			return null;
//
//		return dtoList.stream().map(dto -> convertToEntity(entityClass, dto, ignoreProperties))
//				.collect(Collectors.toList());
//	}
//
//	/**
//	 * Convert Dto to DB Entity
//	 * @param entityClass		required, the Class of entity
//	 * @param dto				required, the record of dto.
//	 * @param ignoreProperties	optional, fields that do not need to be processed during conversion.
//	 * @return the List of converted.
//	 */
//	public <T extends DsBaseDto> Entity convertToEntity(Class<Entity> entityClass, T dto, String... ignoreProperties) {
//
//		if (entityClass == null || dto == null)
//			return null;
//
//		try {
//			Entity entity = entityClass.getDeclaredConstructor().newInstance();
//
//			BeanUtils.copyProperties(dto, entity, ignoreProperties);
//
//			return entity;
//		} catch (Exception e) {
//			log.error("Convert entity " + entityClass + " failed.", e);
//		}
//		return null;
//	}
//
//	public UpdateResult updateById(ID id, Update update, UserDetail userDetail) {
//		Assert.notNull(id, "Id must not be null!");
//
//		return repository.updateFirst(new Query(Criteria.where("_id").is(id)),update, userDetail);
//	}
//
//	public UpdateResult updateById(String id, Update update, UserDetail userDetail) {
//		Assert.notNull(id, "Id must not be null!");
//
//		return repository.updateFirst(new Query(Criteria.where("_id").is(id)),update, userDetail);
//	}
//
//
//	public long updateByWhere(Where where, UpdateDto<Dto> dto, UserDetail userDetail) {
//		Filter filter = new Filter(where);
//		filter.setLimit(0);
//		filter.setSkip(0);
//		Query query = repository.filterToQuery(filter);
//		Entity set = convertToEntity(entityClass, dto.getSet());
//		Entity setOnInsert = convertToEntity(entityClass, dto.getSetOnInsert());
//		return repository.updateByWhere(query, set, setOnInsert, dto.getUnset(), userDetail).getModifiedCount();
//	}
//	public long updateByWhere(Where where, Dto dto, UserDetail userDetail) {
//
//		beforeSave(dto, userDetail);
//		Filter filter = new Filter(where);
//		filter.setLimit(0);
//		filter.setSkip(0);
//		Query query = repository.filterToQuery(filter);
//		Entity entity = convertToEntity(entityClass, dto);
//		UpdateResult updateResult = repository.updateByWhere(query, entity, userDetail);
//		return updateResult.getModifiedCount();
//	}
//
//	public <T extends DsBaseDto>  long upsert(Query query, T dto, UserDetail userDetail) {
//
//		long count = repository.upsert(query, convertToEntity(entityClass, dto), userDetail);
//
//		return count;
//	}
//
//	public Dto  upsertByWhere(Where where, Dto dto, UserDetail userDetail) {
//
//		beforeSave(dto, userDetail);
//		Filter filter = new Filter(where);
//		filter.setLimit(0);
//		filter.setSkip(0);
//		Query query = repository.filterToQuery(filter);
//		repository.upsert(query, convertToEntity(entityClass, dto), userDetail);
//		Optional<Entity> optional = repository.findOne(where, userDetail);
//
//		return optional.map(entity -> convertToDto(entity, dtoClass)).orElse(null);
//	}
//
//	public List<Dto> findAll(Where where, UserDetail userDetail) {
//
//		List<Entity> entities = repository.findAll(where, userDetail);
//		return convertToDto(entities, dtoClass, "password");
//	}
//
//	public UpdateResult update(Query query, Update update, UserDetail userDetail) {
//		return repository.update(query, update, userDetail);
//	}
//
//	public UpdateResult update(Query query, Update update) {
//		return repository.update(query, update);
//	}
//
//	public Entity findAndModify(Query query, Update update, UserDetail userDetail) {
//
//		return repository.findAndModify(query, update, userDetail);
//	}
//
//	public void deleteAll(Query query, UserDetail userDetail) {
//		repository.deleteAll(query, userDetail);
//	}
//
//	public long deleteAll(Query query) {
//		return repository.deleteAll(query);
//	}
//
//	public long count(Where where, UserDetail userDetail) {
//
//		return repository.count(where, userDetail);
//	}
//
//
//	public Dto replaceById(ID id, Dto dto, UserDetail userDetail) {
//		Entity entity = repository.replaceById(new Query(Criteria.where("_id").is(id)), convertToEntity(entityClass, dto), userDetail);
//		return convertToDto(entity, dtoClass);
//	}
//
//	public Dto replaceOrInsert(Dto dto, UserDetail userDetail) {
//		Entity entity = repository.replaceOrInsert(new Query(Criteria.where("_id").is(dto.getId())), convertToEntity(entityClass, dto), userDetail);
//		return convertToDto(entity, dtoClass);
//	}
//
//
//
//}
