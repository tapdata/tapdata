package com.tapdata.tm.system.api.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.modules.vo.ModulesDetailVo;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.entity.TextEncryptionRuleEntity;
import com.tapdata.tm.system.api.enums.OutputType;
import com.tapdata.tm.system.api.enums.RuleType;
import com.tapdata.tm.system.api.repository.TextEncryptionRuleRepository;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.QueryUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 14:40 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TextEncryptionRuleService {
    protected TextEncryptionRuleRepository repository;
    protected ModulesService modulesService;
    protected SettingsService settingsService;

    /**
     * Get the status of 'System Settings/API Distribution Settings/Audit Log Desensitization Switch'
     * */
    public boolean checkAudioSwitchStatus() {
        List<Settings> settings = settingsService.findAll(Query.query(Criteria.where("category").is("ApiServer")
                .and("key").is("audioLogEncryption")).limit(1));
        if (settings.isEmpty()) {
            return false;
        }
        Settings item = settings.get(0);
        return Boolean.TRUE.equals(item.getValue());
    }

    public List<TextEncryptionRuleDto> getById(String ids) {
        if (StringUtils.isBlank(ids)) {
            return new ArrayList<>();
        }
        return getById(new ArrayList<>(List.of(ids)));
    }

    public List<TextEncryptionRuleDto> getById(Collection<String> ids) {
        final List<ObjectId> objectIds = new ArrayList<>();
        for (String idStr : ids) {
            Optional.ofNullable(MongoUtils.toObjectId(idStr)).ifPresent(objectIds::add);
        }
        if (objectIds.isEmpty()) {
            return new ArrayList<>();
        }
        final Query query = Query.query(Criteria.where("_id").in(objectIds));
        final List<TextEncryptionRuleEntity> findResult = repository.findAll(query);
        return findResult
                .stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.equals(e.getDeleted(), 0))
                .map(this::mapToDto)
                .collect(Collectors.toList());
    }

    public Page<TextEncryptionRuleDto> page(Filter filter) {
        final Where where = filter.getWhere();
        final Object name = where.get("name");
        final Query query = new Query();
        final Criteria criteria = Criteria.where("deleted").is(0);
        Optional.ofNullable(name)
                .map(e -> String.valueOf(name))
                .map(String::trim)
                .ifPresent(e -> criteria.and("name").regex(e));
        query.addCriteria(criteria);
        final long count = repository.count(query);
        if (count <= 0) {
            return Page.empty();
        }
        final List<Sort> sortList = QueryUtil.parseOrder(filter);
        query.skip(filter.getSkip())
                .limit(filter.getLimit());
        sortList.forEach(query::with);
        final List<TextEncryptionRuleEntity> all = repository.findAll(query);
        final List<TextEncryptionRuleDto> collect = all.stream()
                .filter(Objects::nonNull)
                .map(this::mapToDto)
                .collect(Collectors.toList());
        return Page.page(collect, count);
    }

    public Boolean create(TextEncryptionRuleDto dto, UserDetail userDetail) {
        dto.setId(new ObjectId());
        final String name = dto.getName();
        final String regex = dto.getRegex();
        if (StringUtils.isBlank(name)) {
            throw new BizException("api.encryption.name.empty");
        }
        if (StringUtils.isBlank(regex)) {
            throw new BizException("api.encryption.regex.empty");
        }
        verifyFormData(dto);
        final TextEncryptionRuleEntity saveEntity = new TextEncryptionRuleEntity();
        final String description = Optional.ofNullable(dto.getDescription()).orElse("");
        final String outputChar = Optional.ofNullable(dto.getOutputChar()).orElse("*");
        final int outputType = Optional.ofNullable(dto.getOutputType()).orElse(OutputType.AUTO.getCode());
        if (outputType == OutputType.CUSTOM.getCode()) {
            int outputCount = Optional.ofNullable(dto.getOutputCount()).orElse(1);
            if (outputCount < 1) {
                outputCount = 1;
            }
            saveEntity.setOutputCount(0);
            saveEntity.setOutputCount(outputCount);
        }
        saveEntity.setDescription(description);
        saveEntity.setOutputChar(outputChar);
        saveEntity.setOutputType(outputType);
        saveEntity.setName(name);
        saveEntity.setRegex(regex);
        saveEntity.setType(RuleType.USER.getCode());
        saveEntity.setOutputType(OutputType.AUTO.getCode());
        synchronized (TextEncryptionRuleService.class) {
            long count = repository.count(Query.query(Criteria.where("name").is(saveEntity.getName())));
            if (count > 0) {
                throw new BizException("api.encryption.name.invalid", saveEntity.getName());
            }
            repository.save(saveEntity, userDetail);
            return true;
        }
    }

    public Boolean update(TextEncryptionRuleDto dto, UserDetail userDetail) {
        final ObjectId id = dto.getId();
        if (null == id) {
            throw new BizException("api.encryption.id.empty");
        }
        verifyFormData(dto);
        final Query query = new Query();
        final TextEncryptionRuleEntity updateEntity = new TextEncryptionRuleEntity();
        query.addCriteria(Criteria.where("_id").is(id));
        Optional.ofNullable(dto.getName()).ifPresent(updateEntity::setName);
        Optional.ofNullable(dto.getDescription()).ifPresent(updateEntity::setDescription);
        Optional.ofNullable(dto.getRegex()).ifPresent(updateEntity::setRegex);
        Optional.ofNullable(dto.getOutputChar()).ifPresent(updateEntity::setOutputChar);
        Optional.ofNullable(dto.getOutputType()).ifPresent(updateEntity::setOutputType);
        Optional.ofNullable(dto.getOutputCount()).ifPresent(updateEntity::setOutputCount);
        updateEntity.setLastUpdBy(userDetail.getUserId());
        updateEntity.setLastUpdAt(new Date());
        updateEntity.setType(RuleType.USER.getCode());
        updateEntity.setOutputType(OutputType.AUTO.getCode());
        if (Objects.nonNull(updateEntity.getName())) {
            synchronized (TextEncryptionRuleService.class) {
                long count = repository.count(Query.query(Criteria.where("name").is(updateEntity.getName()).and("_id").ne(id)));
                if (count > 0) {
                    throw new BizException("api.encryption.name.invalid", updateEntity.getName());
                }
                final UpdateResult updateResult = repository.update(query, updateEntity);
                return updateResult.getMatchedCount() == updateResult.getModifiedCount();
            }
        } else {
            final UpdateResult updateResult = repository.update(query, updateEntity);
            return updateResult.getMatchedCount() == updateResult.getModifiedCount();
        }
    }

    public Boolean batchDelete(String ids, UserDetail userDetail) {
        if (StringUtils.isBlank(ids)) {
            log.info("unable to delete, id is null");
            return false;
        }
        final String[] idArray = ids.split(",");
        if (idArray.length == 0) {
            log.info("unable to delete, id list is empty: {}", ids);
            return false;
        }
        final Set<ObjectId> deleteIds = new HashSet<>();
        for (String idChar : idArray) {
            Optional.ofNullable(MongoUtils.toObjectId(idChar)).ifPresent(deleteIds::add);
        }
        if (deleteIds.isEmpty()) {
            log.info("unable to delete, correct id list is empty: {}", ids);
            return false;
        }
        final Query query = Query.query(Criteria.where("_id").in(new ArrayList<>(deleteIds))
                .and("type").ne(RuleType.SYSTEM.getCode()));
        final Update update = Update.update("deleted", 1);
        final UpdateResult updateResult = repository.update(query, update, userDetail);
        return updateResult.getModifiedCount() == updateResult.getMatchedCount();
    }

    protected TextEncryptionRuleDto mapToDto(TextEncryptionRuleEntity entity) {
        TextEncryptionRuleDto result = new TextEncryptionRuleDto();
        result.setName(entity.getName());
        result.setDescription(entity.getDescription());
        result.setRegex(entity.getRegex());
        result.setOutputChar(entity.getOutputChar());
        result.setOutputType(entity.getOutputType());
        result.setOutputCount(entity.getOutputCount());
        result.setCreateAt(entity.getCreateAt());
        return result;
    }

    void verifyFormData(TextEncryptionRuleDto dto) {
        if (Objects.nonNull(dto.getName()) && dto.getName().length() > 30) {
            throw new BizException("api.encryption.name.too.long", 30);
        }
        if (Objects.nonNull(dto.getRegex()) && dto.getRegex().length() > 512) {
            throw new BizException("api.encryption.regex.too.long", 512);
        }
        if (Objects.nonNull(dto.getDescription()) && dto.getDescription().length() > 512) {
            throw new BizException("api.encryption.description.too.long", 512);
        }
    }

    /**
     * 获取某个api下所有返回字段的文本加密规则配置
     *
     * @param apiId
     * @return
     */
    public Map<String, List<TextEncryptionRuleDto>> getFieldEncryptionRuleByApiId(String apiId) {
        final Path path = getPartByApiId(apiId);
        if (null == path) {
            return new HashMap<>();
        }
        final Set<String> ruleIds = new HashSet<>();
        final Map<String, List<String>> fieldRuleIds = new HashMap<>();
        path.getFields().stream()
                .filter(Objects::nonNull)
                .forEach(e -> {
                    final List<String> textEncryptionRuleIds = e.getTextEncryptionRuleIds();
                    if (CollectionUtils.isNotEmpty(textEncryptionRuleIds)) {
                        ruleIds.addAll(textEncryptionRuleIds);
                        fieldRuleIds.put(e.getFieldName(), textEncryptionRuleIds);
                    }
                });
        return toRule(fieldRuleIds, ruleIds);
    }

    protected Path getPartByApiId(String apiId) {
        final ModulesDetailVo apiInfo = modulesService.findById(apiId);
        final List<Path> paths = apiInfo.getPaths();
        if (paths.isEmpty()) {
            return null;
        }
        return paths.get(0);
    }

    protected Map<String, List<TextEncryptionRuleDto>> toRule(Map<String, List<String>> fieldRuleIds, Set<String> ruleIds) {
        if (fieldRuleIds.isEmpty() || ruleIds.isEmpty()) {
            return new HashMap<>();
        }
        final List<TextEncryptionRuleDto> rules = getById(ruleIds);
        final Map<String, TextEncryptionRuleDto> ruleIdToRuleMap = rules.stream()
                .collect(Collectors.toMap(
                        e -> e.getId().toHexString(),
                        Function.identity()
                ));
        final Map<String, List<TextEncryptionRuleDto>> result = new HashMap<>();
        fieldRuleIds.forEach((fieldName, ruleIdList) -> {
            List<TextEncryptionRuleDto> ruleDtos = ruleIdList.stream()
                    .map(ruleIdToRuleMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (!ruleDtos.isEmpty()) {
                result.put(fieldName, ruleDtos);
            }
        });
        return result;
    }
}
