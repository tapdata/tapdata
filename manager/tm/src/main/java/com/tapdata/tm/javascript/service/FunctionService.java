package com.tapdata.tm.javascript.service;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.javascript.dto.FunctionsDto;
import com.tapdata.tm.javascript.entity.FunctionsEntity;
import com.tapdata.tm.javascript.param.SaveFunctionParam;
import com.tapdata.tm.javascript.repository.FunctionRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

@Service
@Slf4j
public class FunctionService extends BaseService<FunctionsDto, FunctionsEntity, ObjectId, FunctionRepository> {
    public FunctionService(@NonNull FunctionRepository repository) {
        super(repository, FunctionsDto.class, FunctionsEntity.class);
    }


    @Override
    protected void beforeSave(FunctionsDto dto, UserDetail userDetail) {
    }


    @Deprecated
    public List add(List<FunctionsDto> javascriptDtoList, UserDetail userDetail) {

        if (CollectionUtils.isNotEmpty(javascriptDtoList)) {
            for (FunctionsDto functionsDto : javascriptDtoList) {
                if (findByName(functionsDto.getFunctionName()).size() > 0) {
                    log.error("函数有重名的,functionName:{} ", functionsDto.getFunctionName());
                    continue;
                } else {
                    super.save(functionsDto, userDetail);
                }
            }
        }
        return javascriptDtoList;
    }


    public Page findPage(Filter filter, UserDetail userDetail) {

        return super.find(filter,userDetail);
    }


    public FunctionsDto update(FunctionsDto functionsDto, UserDetail userDetail) {

        if (findByName(functionsDto.getFunctionName()).size() > 1) {
            throw new BizException("Function.Name.Exist");
        }
        Where where = Where.where("id", functionsDto.getId()).and("user_id", userDetail.getUserId());
        long updateCount = updateByWhere(where, functionsDto, userDetail);
        return functionsDto;
    }

    public FunctionsDto save(SaveFunctionParam saveFunctionParam,UserDetail userDetail) {
        String functionName = saveFunctionParam.getFunctionName();
        List<FunctionsDto> existedFunctions = findByName(functionName);

        FunctionsDto functionsDto = null;
        functionsDto = BeanUtil.copyProperties(saveFunctionParam, FunctionsDto.class);

        if (CollectionUtils.isNotEmpty(existedFunctions)) {
            //函数名称已存在,直接更
            Query query = new Query();
            query.addCriteria(Criteria.where("function_name").is(functionName));
            Document dbDoc = new Document();
            repository.getMongoOperations().getConverter().write(saveFunctionParam, dbDoc);
            Update update = Update.fromDocument(dbDoc);
            //Update update = Update.fromDBObject(dbDoc,"age","desc");删掉key=age和desc的属性
            update.set("user_id",userDetail.getUserId());
            repository.getMongoOperations().upsert(query, update, FunctionsEntity.class);
        } else  {
            super.save(functionsDto,userDetail);
        }
        return functionsDto;
    }


    public List<FunctionsDto> findByName(String name) {
        Filter filter = new Filter();
        Where where = new Where().and("function_name", name);
        filter.setWhere(where);
        List<FunctionsDto> javascriptDtoList = findAll(filter);
        return javascriptDtoList;
    }

}
