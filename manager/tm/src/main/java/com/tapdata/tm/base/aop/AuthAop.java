package com.tapdata.tm.base.aop;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.*;

@Aspect
@Component
public class AuthAop {

  @Autowired
  private ProductComponent productComponent;


  @Around("execution(public * com.tapdata.tm.base.reporitory.BaseRepository.buildUpdateSet(..)) && target(com.tapdata.tm.ds.repository.DataSourceRepository) && args(.., userDetail)")
  public Object dataSourceRepository_buildUpdateSet(ProceedingJoinPoint pjp, UserDetail userDetail) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    String tempUserId = null;
    try {
      if (userDetail != null && !userDetail.isFreeAuth()) {
        tempUserId = userDetail.getUserId();
        userDetail.setUserId(null);
      }
      return pjp.proceed();
    } finally {
      if (tempUserId != null) {
        userDetail.setUserId(tempUserId);
      }
    }
  }

  @Around("execution(public * com.tapdata.tm.base.service.BaseService.*(..)) && target(com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService) && args(.., filter, user)")
  public Object metadataDefinitionService_filter(ProceedingJoinPoint pjp, Filter filter, UserDetail user) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    boolean isChange = false;
    try {
      if (user != null && !user.isRoot() && !user.isFreeAuth()) {
        user.setFreeAuth();
        filter.setWhere(new Where()
                .and("and", new ArrayList<Map<String, Object>>() {{
                  add(filter.getWhere());
                  add(new HashMap<String, Object>() {{
                    put("user_id", new HashMap<String, Object>() {{
                      put("$in", Arrays.asList(user.getUserId(), null, ""));
                    }});

                  }});
                }}));
        isChange = true;
      }
      return pjp.proceed();
    } finally {
      if (isChange) {
        user.setAuth();
      }
    }
  }

  @Around("execution(public * com.tapdata.tm.base.service.BaseService.*(..)) && target(com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService) && args(.., query, user)")
  public Object metadataDefinitionService_query(ProceedingJoinPoint pjp, Query query, UserDetail user) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    boolean isChange = false;
    try {
      if (user != null && !user.isRoot() && !user.isFreeAuth()) {
        query.addCriteria(new Criteria().and("user_id").in(user.getUserId(), null, ""));
        isChange = true;
      }
      return pjp.proceed();
    } finally {
      if (isChange) {
        user.setAuth();
      }
    }
  }
  @Around("execution(public * com.tapdata.tm.base.service.BaseService.*(..)) && target(com.tapdata.tm.apiServer.service.ApiServerService) && args(.., userDetail)")
  public Object apiServerService(ProceedingJoinPoint pjp, UserDetail userDetail) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    boolean isChange = false;
    try {
      if (userDetail != null && !userDetail.isFreeAuth()) {
        userDetail.setFreeAuth();
        isChange = true;
      }
      return pjp.proceed();
    } finally {
      if (isChange) {
        userDetail.setAuth();
      }
    }
  }

  @Around("execution(public * com.tapdata.tm.metadatainstance.service.MetadataInstancesService.*(..)) && args(..,metadataInstancesDto, userDetail)")
  public Object metadataInstancesService_updateByWhere(ProceedingJoinPoint pjp, MetadataInstancesDto metadataInstancesDto, UserDetail userDetail) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    return setUserId(pjp, metadataInstancesDto, userDetail);
  }

  @Around("execution(public * com.tapdata.tm.base.service.BaseService.*(..)) && target(com.tapdata.tm.metadatainstance.service.MetadataInstancesService) && args(..,metadataInstancesDto, userDetail)")
  public Object metadataInstancesService_BaseService(ProceedingJoinPoint pjp, MetadataInstancesDto metadataInstancesDto, UserDetail userDetail) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    return setUserId(pjp, metadataInstancesDto, userDetail);
  }

  @Around("execution(public * com.tapdata.tm.metadatainstance.service.MetadataInstancesService.bulkUpsetByWhere(..)) && args(metadataInstancesDtos, userDetail)")
  public Object metadataInstancesService_bulkUpsetByWhere(ProceedingJoinPoint pjp, List<MetadataInstancesDto> metadataInstancesDtos, UserDetail userDetail) throws Throwable {
    if (productComponent.isCloud()) {
      return pjp.proceed();
    }
    return setUserId(pjp, metadataInstancesDtos.get(0), userDetail);
  }

  private static Object setUserId(ProceedingJoinPoint pjp, MetadataInstancesDto metadataInstancesDto, UserDetail userDetail) throws Throwable {
    String tempUserId = null;
    String tempUserName = null;
    try {
      if (metadataInstancesDto != null && metadataInstancesDto.getSource() != null && userDetail != null) {
        SourceDto source = metadataInstancesDto.getSource();
        if (!StringUtils.equals(source.getUser_id(), userDetail.getUserId())) {
          tempUserId = userDetail.getUserId();
          tempUserName = userDetail.getUsername();
          userDetail.setUserId(source.getUser_id());
          userDetail.setUsername(source.getCreateUser());
        }
      }
      return pjp.proceed();
    } finally {
      if (tempUserId != null) {
        userDetail.setUserId(tempUserId);
      }
      if (tempUserName != null) {
        userDetail.setUsername(tempUserName);
      }
    }
  }

}
