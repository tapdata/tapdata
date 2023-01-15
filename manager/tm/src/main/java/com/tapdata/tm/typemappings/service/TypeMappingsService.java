package com.tapdata.tm.typemappings.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.typemappings.constant.TypeMappingDirection;
import com.tapdata.tm.typemappings.dto.DatabaseTypeResDto;
import com.tapdata.tm.typemappings.dto.TypeMappingsDto;
import com.tapdata.tm.typemappings.entity.TypeMappingsEntity;
import com.tapdata.tm.typemappings.repository.TypeMappingsRepository;
import com.tapdata.tm.config.security.UserDetail;

import java.util.*;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class TypeMappingsService extends BaseService<TypeMappingsDto, TypeMappingsEntity, ObjectId, TypeMappingsRepository> {

    @Autowired
    private DataSourceDefinitionService definitionService;
    public TypeMappingsService(@NonNull TypeMappingsRepository repository) {
        super(repository, TypeMappingsDto.class, TypeMappingsEntity.class);
    }

    protected void beforeSave(TypeMappingsDto typeMappings, UserDetail user) {

    }

    public String findByDataType(String databaseType, UserDetail user) {
        DataSourceDefinitionDto definitionDto = definitionService.getByDataSourceType(databaseType, user);
        if (definitionDto != null) {
            return definitionDto.getExpression();
        }

        return null;
    }


    public List<DatabaseTypeResDto> findByDataType(String databaseType){
        if (StringUtils.isBlank(databaseType)){
            throw new BizException("DatabaseType is blank");
        }

        List<DatabaseTypeResDto> result = new ArrayList<>();
        List<TypeMappingsDto> typeMappingsDtos = findAll(Query.query(Criteria.where("databaseType").is(databaseType)
                .and("direction").in(TypeMappingDirection.ALL.name(), TypeMappingDirection.TO_DATATYPE.name())));

        Map<String, List<DatabaseTypeResDto.Rule>> map = new HashMap<>();
        Set<String> set = new HashSet<>();
        for (TypeMappingsDto typeMappingsDto : typeMappingsDtos) {
            if (set.contains("" + typeMappingsDto.getDbType()
                    + typeMappingsDto.getMinPrecision()
                    + typeMappingsDto.getMaxPrecision()
                    + typeMappingsDto.getMinScale()
                    + typeMappingsDto.getMaxScale())) {
                continue;
            }
            set.add("" + typeMappingsDto.getDbType()
                    + typeMappingsDto.getMinPrecision()
                    + typeMappingsDto.getMaxPrecision()
                    + typeMappingsDto.getMinScale()
                    + typeMappingsDto.getMaxScale());
            if (!map.containsKey(typeMappingsDto.getDbType())) {
                map.put(typeMappingsDto.getDbType(), new ArrayList<>());
            }
            List<DatabaseTypeResDto.Rule> rules = map.get(typeMappingsDto.getDbType());
            DatabaseTypeResDto.Rule rule = new DatabaseTypeResDto.Rule();
            rule.setMinPrecision(typeMappingsDto.getMinPrecision());
            rule.setMaxPrecision(typeMappingsDto.getMaxPrecision());
            rule.setMinScale(typeMappingsDto.getMinScale());
            rule.setMaxScale(typeMappingsDto.getMaxScale());
            rules.add(rule);
        }

        map.forEach((key, value) -> {
            DatabaseTypeResDto databaseTypeResDto = new DatabaseTypeResDto();
            databaseTypeResDto.setDbType(key);
            databaseTypeResDto.setRules(value);
            result.add(databaseTypeResDto);
        });

        return result;
    }

    /**
     * 根据 数据库类型查找对应的类型映射规则
     * @param databaseType 数据库类型
     * @param direction 转换方向：TO_DATABASE, TO_TAPTYPE
     * @return
     */
    @Cacheable(cacheManager = "memoryCache", cacheNames = "TypeMapping", key = "#databaseType + '-' + #direction.name()", unless = "#result == null || #result.size() == 0")
    public Map<String, List<TypeMappingsEntity>> getTypeMapping(String databaseType, TypeMappingDirection direction) {
        if (StringUtils.isEmpty(databaseType) || direction == null) {
            log.warn("Missing params, databaseType or direction can't be empty.");
            return Collections.emptyMap();
        }

        log.debug("Loader type mapping rule for {}, {}", databaseType, direction.name());

        List<TypeMappingsEntity> typeMappings = repository.findAll(Query.query(
                Criteria.where("databaseType").is(databaseType)
                        .and("direction").in(direction.name(), TypeMappingDirection.ALL.name())));

        if(typeMappings == null || typeMappings.size() == 0) {
            return Collections.emptyMap();
        }

        // direction=TO_DATATYPE: STRING-* > [TypeMappingsEntity]
        // direction=TO_TAPTYPE: String-* > [TypeMappingsEntity]
        // direction=ALL: String-* > [TypeMappingsEntity], STRING-* > [TypeMappingsEntity]
        Map<String, List<TypeMappingsEntity>> cacheMapping = new HashMap<>();

        typeMappings.forEach(typeMapping -> {
            if (StringUtils.isEmpty(typeMapping.getVersion())) {
                typeMapping.setVersion("*");
            }
            String type = direction == TypeMappingDirection.TO_DATATYPE ?
                    typeMapping.getTapType() : typeMapping.getDbType();
            String cacheKey = String.format("%s-%s", type, typeMapping.getVersion());
            if(!cacheMapping.containsKey(cacheKey))
                cacheMapping.put(cacheKey, new ArrayList<>());
            cacheMapping.get(cacheKey).add(typeMapping);
        });

        return cacheMapping;
    }

    /**
     * 清理换成
     * @param databaseType
     * @param direction
     */
    @CacheEvict(cacheManager = "memoryCache", cacheNames = "TypeMapping", key = "#databaseType + '-' + #direction.name()")
    public void clearTypeMappingCache(String databaseType, TypeMappingDirection direction){}

    public void deleteAll(Where where) {
        Criteria criteria = repository.whereToCriteria(where);
        deleteAll(new Query(criteria));
    }
}
