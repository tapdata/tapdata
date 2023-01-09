package com.tapdata.tm.behavior.aop;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.behavior.BehaviorCode;
import com.tapdata.tm.behavior.entity.BehaviorEntity;
import com.tapdata.tm.behavior.service.BehaviorService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/12/9 上午10:13
 */
@Aspect
@Component
@Slf4j
public class BehaviorAop {

    @Autowired
    private BehaviorService behaviorService;

    /**
     * pointcut on create connection
     */
    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.add*(..))")
    public void createConnection() { }

    @Around("createConnection()")
    public Object onCreateConnection(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = pjp.proceed();

        try {
            Object[] args = pjp.getArgs();
            if (args != null && args.length == 2
                    && args[0] instanceof DataSourceConnectionDto && args[1] instanceof UserDetail
                    && retVal instanceof DataSourceConnectionDto) {
                //DataSourceConnectionDto dataSourceConnectionDto = (DataSourceConnectionDto) args[0];
                UserDetail userDetail = (UserDetail) args[1];
                DataSourceConnectionDto result = (DataSourceConnectionDto) retVal;

                BehaviorEntity behavior = new BehaviorEntity();
                behavior.setCode(BehaviorCode.createConnection.name());
                behavior.setAttrs(new HashMap<>());
                behavior.getAttrs().put("id", result.getId());
                behavior.getAttrs().put("status", result.getStatus());
                behavior.getAttrs().put("connectionType", result.getConnection_type());
                behavior.getAttrs().put("databaseType", result.getDatabase_type());
                behaviorService.trace(behavior, userDetail);
            }
        } catch (Exception e) {
            log.error("Trace create connection behavior failed", e);
        }

        return retVal;
    }

    /**
     * pointcut on update connection and test connection.
     */
    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.update(..))")
    public void updateConnection(){}

    @Around("updateConnection()")
    public Object onUpdateConnection(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = pjp.proceed();
        try {
            Object[] args = pjp.getArgs();

            if (args != null && args.length == 2
                    && args[0] instanceof UserDetail && args[1] instanceof DataSourceConnectionDto
                    && retVal instanceof DataSourceConnectionDto) {
                UserDetail userDetail = (UserDetail) args[0];
                DataSourceConnectionDto dataSourceConnectionDto = (DataSourceConnectionDto) args[1];
                DataSourceConnectionDto result = (DataSourceConnectionDto) retVal;

                BehaviorEntity behavior = new BehaviorEntity();
                if (DataSourceConnectionDto.STATUS_TESTING.equals(dataSourceConnectionDto.getStatus())
                        && StringUtils.isBlank(dataSourceConnectionDto.getName())
                        && dataSourceConnectionDto.getConfig() == null) {
                    // test connection
                    behavior.setCode(BehaviorCode.testConnection.name());
                } else {
                    // edit connection
                    behavior.setCode(BehaviorCode.editConnection.name());
                }
                behavior.setAttrs(new HashMap<>());
                behavior.getAttrs().put("id", result.getId());
                behavior.getAttrs().put("status", result.getStatus());
                behavior.getAttrs().put("connectionType", result.getConnection_type());
                behavior.getAttrs().put("databaseType", result.getDatabase_type());
                behaviorService.trace(behavior, userDetail);
            }
        } catch (Exception e) {
            log.error("Trace edit connection behavior failed", e);
        }
        return retVal;
    }

    /**
     * pointcut on update connection test result.
     */
    @Pointcut("execution(* com.tapdata.tm.ds.service.impl.DataSourceService.upsertByWhere(..))")
    public void testConnection(){}

    @Around("testConnection()")
    public Object onTestConnection(ProceedingJoinPoint pjp) throws Throwable{
        Object retVal = pjp.proceed();
        try {
            Object[] args = pjp.getArgs();

            if (args != null && args.length == 4
                    && args[0] instanceof Where
                    && args[1] instanceof Document
                    && args[2] == null
                    && args[3] instanceof UserDetail) {
                UserDetail userDetail = (UserDetail) args[3];
                Document update = (Document) args[1];
                Where where = (Where) args[0];

                String status = null;
                Object responseBody = null;
                ObjectId id = null;
                if (where.containsKey("_id")) {
                    id = (ObjectId) where.get("_id");
                }
                if( update.get("$set") instanceof Map) {
                    Map<String, Object> set = (Map<String, Object>) update.get("$set");
                    if(set.containsKey("status")) {
                        status = (String) set.get("status");
                    }
                    if (set.containsKey("response_body.validate_details")) {
                        responseBody = set.get("response_body.validate_details");
                    }
                }
                if (id != null && status != null && responseBody != null) {
                    BehaviorEntity behavior = new BehaviorEntity();
                    behavior.setCode(BehaviorCode.testConnection.name());
                    behavior.setAttrs(new HashMap<>());
                    behavior.getAttrs().put("id", id);
                    behavior.getAttrs().put("status", status);
                    behaviorService.trace(behavior, userDetail);
                }
            }
        } catch (Exception e) {
            log.error("Trace test connection behavior failed", e);
        }
        return retVal;
    }

    /**
     * pointcut on create task
     */
    @Pointcut("execution(* com.tapdata.tm.task.service.TaskService.create(..))")
    public void createTask() {}

    @Around("createTask()")
    public Object onCreateTask(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = pjp.proceed();
        try {
            Object[] args = pjp.getArgs();

            if (args != null && args.length == 2
                    && args[0] instanceof TaskDto
                    && args[1] instanceof UserDetail) {
                TaskDto taskDto = (TaskDto) retVal;//(TaskDto) args[0];
                UserDetail userDetail = (UserDetail) args[1];
                BehaviorEntity behavior = new BehaviorEntity();
                behavior.setCode(BehaviorCode.createTask.name());
                behavior.setAttrs(new HashMap<>());
                behavior.getAttrs().put("id", taskDto.getId());
                behavior.getAttrs().put("name", taskDto.getName());
                behavior.getAttrs().put("syncType", taskDto.getSyncType());
                behavior.getAttrs().put("type", taskDto.getType());
                behavior.getAttrs().put("status", taskDto.getStatus());
                behaviorService.trace(behavior, userDetail);
            }
        } catch (Exception e) {
            log.error("Trace create task behavior failed", e);
        }
        return retVal;
    }

    /**
     * pointcut on create task
     */
    @Pointcut("execution(* com.tapdata.tm.task.service.TaskService.confirmById(..))")
    public void confirmTask() {}

    @Around("confirmTask()")
    public Object onConfirmTask(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = pjp.proceed();

        try {
            Object[] args = pjp.getArgs();

            if (args != null && args.length == 3
                    && args[0] instanceof TaskDto
                    && args[1] instanceof UserDetail) {
                TaskDto taskDto = (TaskDto) retVal;//(TaskDto) args[0];
                UserDetail userDetail = (UserDetail) args[1];
                BehaviorEntity behavior = new BehaviorEntity();
                behavior.setCode(BehaviorCode.editTask.name());
                behavior.setAttrs(new HashMap<>());
                behavior.getAttrs().put("id", taskDto.getId());
                behavior.getAttrs().put("name", taskDto.getName());
                behavior.getAttrs().put("syncType", taskDto.getSyncType());
                behavior.getAttrs().put("type", taskDto.getType());
                behavior.getAttrs().put("status", taskDto.getStatus());
                behaviorService.trace(behavior, userDetail);
            }
        } catch (Exception e) {
            log.error("Trace create task behavior failed", e);
        }

        return retVal;
    }

    /**
     * pointcut on create task
     */
    @Pointcut("execution(* com.tapdata.tm.task.service.TaskService.remove(..))")
    public void deleteTask() {}

    @Around("deleteTask()")
    public Object onDeleteTask(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = pjp.proceed();

        try {
            Object[] args = pjp.getArgs();

            if (args != null && args.length == 2
                    && args[0] instanceof ObjectId
                    && args[1] instanceof UserDetail) {
                TaskDto taskDto = (TaskDto) retVal;//(TaskDto) args[0];
                UserDetail userDetail = (UserDetail) args[1];
                BehaviorEntity behavior = new BehaviorEntity();
                behavior.setCode(BehaviorCode.deleteTask.name());
                behavior.setAttrs(new HashMap<>());
                behavior.getAttrs().put("id", taskDto.getId());
                behavior.getAttrs().put("name", taskDto.getName());
                behavior.getAttrs().put("syncType", taskDto.getSyncType());
                behavior.getAttrs().put("type", taskDto.getType());
                behavior.getAttrs().put("status", taskDto.getStatus());
                behaviorService.trace(behavior, userDetail);
            }
        } catch (Exception e) {
            log.error("Trace create task behavior failed", e);
        }

        return retVal;
    }

    /**
     * pointcut on batch delete task
     */
    @Pointcut("execution(* com.tapdata.tm.task.service.TaskService.batchDelete(..))")
    public void batchDeleteTask() { }

    @Around("batchDeleteTask()")
    public Object onBatchDeleteTask(ProceedingJoinPoint pjp) throws Throwable {
        Object retVal = pjp.proceed();

        try {
            Object[] args = pjp.getArgs();

            if (args != null && args.length == 4
                    && args[0] instanceof List
                    && args[1] instanceof UserDetail) {
                List<ObjectId> deleteTasks = (List<ObjectId>) args[0];
                UserDetail userDetail = (UserDetail) args[1];
                deleteTasks.forEach(id -> {
                    BehaviorEntity behavior = new BehaviorEntity();
                    behavior.setCode(BehaviorCode.deleteTask.name());
                    behavior.setAttrs(new HashMap<>());
                    behavior.getAttrs().put("id", id);
                    behaviorService.trace(behavior, userDetail);
                });

            }
        } catch (Exception e) {
            log.error("Trace create task behavior failed", e);
        }

        return retVal;
    }
}
