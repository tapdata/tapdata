package com.tapdata.tm.typemappings.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.typemappings.constant.DataTypeMatcher;
import com.tapdata.tm.typemappings.constant.DataTypeOperator;
import com.tapdata.tm.typemappings.dto.DataTypeSupportDto;
import com.tapdata.tm.typemappings.entity.DataTypeSupportEntity;
import com.tapdata.tm.typemappings.repository.DataTypeRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午2:20
 */
@Service
@Slf4j
public class DataTypeSupportService extends BaseService<DataTypeSupportDto, DataTypeSupportEntity, ObjectId, DataTypeRepository> {

    private Map<DataTypeOperator, DataTypeMatcher> matchers = new HashMap<>();

    public DataTypeSupportService(@NonNull DataTypeRepository repository) {
        super(repository, DataTypeSupportDto.class, DataTypeSupportEntity.class);

        matchers.put(DataTypeOperator.regex, getRegexMatcher());
        matchers.put(DataTypeOperator.equal, getEqualMatcher());
        matchers.put(DataTypeOperator.ignoreCaseEqual, getIgnoreCaseEqualMatcher());
        matchers.put(DataTypeOperator.in, getInMatcher());
        matchers.put(DataTypeOperator.ignoreCaseIn, getIgnoreCaseInMatcher());
    }

    @Override
    protected void beforeSave(DataTypeSupportDto dto, UserDetail userDetail) {

    }

    @Cacheable(cacheManager = "memoryCache", cacheNames = "DataTypeSupport",
            key = "'DataTypeSupport-' + #sourceDbType + '-' + #targetDbType + '-' + #dataType")
    public boolean supportDataType(String sourceDbType, String targetDbType, String dataType) {

        Criteria criteria = Criteria.where("sourceDbType").is(sourceDbType).and("targetDbType").is(targetDbType);

        List<DataTypeSupportEntity> dataTypes = repository.findAll(Query.query(criteria));

        for (int i = 0; i < dataTypes.size(); i++) {
            DataTypeSupportEntity dataTypeSupportEntity = dataTypes.get(i);
            String operatorStr = dataTypeSupportEntity.getOperator();
            DataTypeOperator operator = DataTypeOperator.valueOf(operatorStr != null ? operatorStr : "equal");

            DataTypeMatcher matcher = matchers.get(operator);
            boolean matched = matcher.match(dataTypeSupportEntity.getExpression(), dataType);
            if (matched) {
                return dataTypeSupportEntity.isSupport();
            }
        }
        return true;
    }

    @CacheEvict(cacheManager = "memoryCache", cacheNames = "DataTypeSupport")
    public void clearCache(){}

    private Collection<String> getExpressionAsList(Object expression) {
        if (expression instanceof Collection<?>) {
            return (Collection<String>) expression;
        }
        if (expression != null)
            return Collections.singletonList(expression.toString());
        return Collections.emptyList();
    }

    private String getExpressionAsString(Object expression) {
        if (expression instanceof String) {
            return (String) expression;
        }
        return expression != null ? expression.toString() : null;
    }



    private DataTypeMatcher getIgnoreCaseInMatcher() {
        return ((expression, dataType) -> {
            Collection<String> expressions = getExpressionAsList(expression);
            if (expressions != null) {
                for (String exp : expressions) {
                    if (exp.equalsIgnoreCase(dataType)) {
                        return true;
                    }
                }
            }
            return false;
        });
    }

    private DataTypeMatcher getInMatcher() {
        return ((expression, dataType) -> {
            Collection<String> expressions = getExpressionAsList(expression);
            return expressions != null && expressions.contains(dataType);
        });
    }

    private DataTypeMatcher getIgnoreCaseEqualMatcher() {
        return ((expression, dataType) -> {
            String strExpression = getExpressionAsString(expression);
            return strExpression != null && strExpression.equalsIgnoreCase(dataType);
        });
    }

    private DataTypeMatcher getEqualMatcher() {
        return ((expression, dataType) -> {
            String strExpression = getExpressionAsString(expression);
            return strExpression != null && strExpression.equals(dataType);
        });
    }

    private DataTypeMatcher getRegexMatcher() {
        return (expression, dataType) -> {

            String strExpression = getExpressionAsString(expression);
            if (strExpression != null) {
                Pattern pattern = Pattern.compile(strExpression, Pattern.CASE_INSENSITIVE);
                return pattern.matcher(dataType).matches();
            }
            return false;
        };
    }

}
