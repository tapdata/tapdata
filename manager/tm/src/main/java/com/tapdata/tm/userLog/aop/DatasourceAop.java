package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.UpdateTagsDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.CollectionsUtils;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;


/**
 * 需要区别出来自flowEngin的请求，
 * 如果是来自flowEngin请求，请求头User-Agent:  里会带Java的字符串
 * 如果是来自浏览器用户的点击，User-Agent:带的就是各个浏览器自己的属性。从而去吧
 * <p>
 * // 连接
 * { label: '创建连接', value: 'connection_create', desc: '创建了连接【@{parameter1}】' },
 * { label: '编辑连接', value: 'connection_update', desc: '编辑了连接【@{parameter1}】的配置信息' },
 * { label: '复制连接', value: 'connection_copy', desc: '复制了连接[${parameter1}]为【@{parameter2}】' },
 * { label: '删除连接', value: 'connection_delete', desc: '删除了连接【${parameter1}】' },
 */
@Aspect
@Component
@Slf4j
public class DatasourceAop {

    @Autowired
    UserLogService userLogService;

    @Autowired
    UserService userService;


    @Autowired
    DataSourceService dataSourceService;


    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.add(..))")
    public void addPointCut() {

    }

    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.update(..))")
    public void updatePointCut() {

    }


    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.updateTag(..))")
    public void updateTagPointCut() {

    }


    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.updateByWhere(..))")
    public void updateByWherePointCut() {

    }

    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.upsertByWhere(..))")
    public void upsertByWherePointCut() {

    }

    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.copy(..))")
    public void copyPointCut() {

    }

    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.delete(..))")
    public void deletePointCut() {

    }

    /**
     * 拦截 DataSourceService delete方法
     *
     * @param joinPoint
     * @throws Throwable
     */
    @Around("deletePointCut()")
    public void afterDeleteReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs();
        UserDetail userDetail = (UserDetail) args[0];
        String id = (String) args[1];
        DataSourceConnectionDto dataSourceConnectionDto = (DataSourceConnectionDto) joinPoint.proceed(args);
        if (null != dataSourceConnectionDto) {
            userLogService.addUserLog(Modular.CONNECTION, Operation.DELETE, userDetail, id, dataSourceConnectionDto.getName());
        }
    }

    /**
     * 拦截 DataSourceService updateByWhere  和upsertByWhere   方法
     * 主要拦截upsertByWhere方法，updateByWhere没有被controller 调用
     *
     * @return
     */
    @After("updateByWherePointCut()||upsertByWherePointCut()")
    public Object afterUpdateByWherePointcut(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return null;
        }
        Object[] args = joinPoint.getArgs();
        Where where = (Where) args[0];
        UserDetail userDetail = (UserDetail) args[3];

        List<DataSourceConnectionDto> datasourceList = dataSourceService.findAll(where, userDetail);

        Operation operationType = null;
        if (CollectionUtils.isNotEmpty(datasourceList)) {
            if (datasourceList.size() > 1) {
                operationType = Operation.BATCH_UPDATE;

            } else {
                operationType = Operation.UPDATE;
            }
            List<String> names = datasourceList.stream().map(DataSourceConnectionDto::getName).collect(Collectors.toList());
            List<ObjectId> dataFlowObjectIds = datasourceList.stream().map(DataSourceConnectionDto::getId).collect(Collectors.toList());


            String resetNames = StringUtils.join(names, ",");
            String resetIds = StringUtils.join(CollectionsUtils.ObjectIdToString(dataFlowObjectIds), ",");

            userLogService.addUserLog(Modular.CONNECTION, operationType, userDetail, resetIds, resetNames);
        }
        return null;
    }


    /**
     * 拦截.DataSourceService.copy 方法
     * 因为需要拿到修改前后的任务名称，所以只能用around方法
     */
    @Around("copyPointCut()")
    public Object afterCopyReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        log.info("拦截  DataSourceService copy");
        DataSourceConnectionDto result = null;
        Object[] args = joinPoint.getArgs();
        result = (DataSourceConnectionDto) joinPoint.proceed(args);
        if (shouldRecord()) {
            //获取方法参数值数组
            String id = (String) args[1];
            UserDetail userDetail = (UserDetail) args[0];
            DataSourceConnectionDto originDatasourceDto = dataSourceService.findById(new ObjectId(id));
            userLogService.addUserLog(Modular.CONNECTION, Operation.COPY, userDetail, id, originDatasourceDto.getName(), result.getName(), false);
        }
        return result;
    }

    //设置update方法为后置通知
 /*   @AfterReturning(value = "copyPointCut()", returning = "result")
    public void afterCopyReturning(Object result) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return ;
        }
        log.info("执行了 更新链接 操作");
        try {
            DataSourceConnectionDto dataSourceConnectionDto = (DataSourceConnectionDto) result;
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(dataSourceConnectionDto.getUserId()));
            userLogService.addUserLog(Modular.CONNECTION, dataSourceConnectionDto.getName(), OperationType.COPY, userDetail, dataSourceConnectionDto.getId().toString());
        } catch (Exception e) {
            log.error("保存  更新链接  日志异常", e);
        }
    }*/


    /**
     * 传入参数是  Map<String, Object> dto, UserDetail userDetail
     *
     * @return
     */
    @After("updateTagPointCut()")
    public Object afterUpdateTagPointcut(JoinPoint joinPoint) {
        log.info("拦截  DataSourceService update");
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return null;
        }
        Object[] args = joinPoint.getArgs();

        UserDetail userDetail = (UserDetail) args[0];
        UpdateTagsDto updateTagsDto = (UpdateTagsDto) args[1];

        List<String> ids = updateTagsDto.getId();

        List<ObjectId> objectIdList = ids.stream().map(ObjectId::new).collect(Collectors.toList());

        Operation operationType = null;

        Query query = Query.query(Criteria.where("_id").in(objectIdList));
        List<DataSourceEntity> dataFlowList = dataSourceService.findAll(query, userDetail);

        if (CollectionUtils.isNotEmpty(dataFlowList)) {
            if (dataFlowList.size() > 1) {
                operationType = Operation.BATCH_UPDATE;

            } else {
                operationType = Operation.UPDATE;
            }
            List<String> names = dataFlowList.stream().map(DataSourceEntity::getName).collect(Collectors.toList());
            List<ObjectId> dataFlowObjectIds = dataFlowList.stream().map(DataSourceEntity::getId).collect(Collectors.toList());

            String resetNames = StringUtils.join(names, ",");
            String resetIds = StringUtils.join(CollectionsUtils.ObjectIdToString(dataFlowObjectIds), ",");

            userLogService.addUserLog(Modular.CONNECTION, operationType, userDetail, resetIds, resetNames);
        }

        return null;
    }


    /**
     * 拦截 DataSourceService update 方法
     * 点击测试数据源  和编辑数据源的时候，都会调用 DataSourceService update
     * 如果是测试连接，前端传{"status":"testing"}
     * 只需要记录编辑的动作，测试数据源不用记录
     * <p>
     * 修改名称，和修改其他配置，要做不一样的操作
     *
     * @param joinPoint
     */
    @Around(value = "updatePointCut()")
    public DataSourceConnectionDto afterUpdateReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        log.info("拦截  DataSourceService update");

        DataSourceConnectionDto dtoAfter = null;
        Object[] args = joinPoint.getArgs();

        //业务方法执行完毕
        if (shouldRecord()) {
            log.info("是来自用户操作");

            UserDetail userDetail = (UserDetail) args[0];

            //dtoBefore  的属性会随着业务方法的操作，而变动
            DataSourceConnectionDto dtoBefore = (DataSourceConnectionDto) args[1];
            String statusBefore=dtoBefore.getStatus();

            //先获取更新前的操作
            String beforeName = dataSourceService.findById(dtoBefore.getId()).getName();

            //执行更新
            dtoAfter = (DataSourceConnectionDto) joinPoint.proceed(args);

            //获取更新后的名称
            String afterName = dtoAfter.getName();

            if ("testing".equals(statusBefore)) {
                log.info(" 测试连接，不记录日志");
            } else if (beforeName.equals(afterName)) {
                log.info("不是修改名称");
                userLogService.addUserLog(Modular.CONNECTION, Operation.UPDATE, userDetail, dtoAfter.getId().toString(), afterName);
            } else if (!beforeName.equals(afterName)) {
                log.info("修改了名称");
                userLogService.addUserLog(Modular.CONNECTION, Operation.UPDATE, userDetail, dtoBefore.getId().toString(),afterName, beforeName,  true);
            }
        }
        return dtoAfter;
    }


    /**
     * 拦截
     *
     * @param result
     */
    @AfterReturning(value = "addPointCut()", returning = "result")
    public void afterSaveReturning(Object result) {
        if (!shouldRecord()) {
            log.info("不是来自用户操作");
            return;
        }
        log.info("执行了 创建链接 操作");
        DataSourceConnectionDto dataSourceConnectionDto = (DataSourceConnectionDto) result;
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(dataSourceConnectionDto.getUserId()));
        userLogService.addUserLog(Modular.CONNECTION, Operation.CREATE, userDetail, dataSourceConnectionDto.getId().toString(), dataSourceConnectionDto.getName());

    }


    /**
     * 可能拦截不到egine发来的userAgent
     *
     * @return
     */
    private Boolean shouldRecord() {
        try {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
            HttpServletRequest request = attributes.getRequest();
            String userAgent = request.getHeader("User-Agent");
            log.info("接收到的 userAgent：{} ", userAgent);
            return !StringUtils.isNotEmpty(userAgent) || (!userAgent.contains("Java") && !userAgent.contains("java") && !userAgent.contains("nodejs"));
        } catch ( Exception e) {
            return false;
        }
    }

}
