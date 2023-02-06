package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllReqDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllResDto;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
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
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 拦截任务方法，以便写入操作日志
 */
@Aspect
@Component
@Slf4j
public class DataFlowAop {
    @Autowired
    UserLogService userLogService;

    @Autowired
    UserService userService;

    @Autowired
    DataFlowService dataFlowService;

    @Autowired
    MessageService messageService;


    @Pointcut("execution(* com.tapdata.tm.dataflow.service.DataFlowService.save(..))")
    public void savePointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.dataflow.service.DataFlowService.patch(..))")
    public void patchPointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.dataflow.service.DataFlowService.copyDataFlow(..))")
    public void copyPointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.dataflow.service.DataFlowService.updateById(..))")
    public void updateByIdPointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.base.service.BaseService.updateByWhere(..))  && target(com.tapdata.tm.dataflow.service.DataFlowService)")
    public void updateByWherePointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.dataflow.service.DataFlowService.resetDataFlow(..))")
    public void resetDataFlowPointcut() {

    }

    @Pointcut("execution(* com.tapdata.tm.dataflow.service.DataFlowService.removeDataFlow(..))")
    public void removeDataFlowPointCut() {

    }

    /**
     * 传入参数是  Map<String, Object> dto, UserDetail userDetail
     *
     * @return
     */
    @After("updateByIdPointcut()")
    public Object afterUpdateByIdPointcut(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            return null;
        }
        String methodName = joinPoint.getSignature().getName();
        Object[] args = joinPoint.getArgs();

        String id = (String) args[0];
        DataFlowDto dataFlowDto = (DataFlowDto) args[1];
        UserDetail userDetail = (UserDetail) args[2];
        userLogService.addUserLog(Modular.MIGRATION, Operation.UPDATE, userDetail, id, dataFlowDto.getName());

        return null;
    }


    @AfterReturning(value = "savePointcut()", returning = "result")
    public void afterSaveReturning(Object result) {
        if (!shouldRecord()) {
            return;
        }

        log.info("执行了任务创建操作");
        DataFlowDto dtoMap = (DataFlowDto) result;
        ObjectId id = dtoMap.getId();
        String userId = dtoMap.getUserId();
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
        String name = dtoMap.getName();
        userLogService.addUserLog(Modular.MIGRATION, Operation.CREATE, userDetail, id.toString(), name);

    }

    /**
     * 云版任务启动或者停止
     *
     * @param joinPoint
     */
    @Around("patchPointcut()")
    public Map<String, Object> afterPatchReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        log.info("云版执行了任务操作");
        Object[] args = joinPoint.getArgs();
        Map<String, Object> result = null;

        Map<String, Object> map = (Map) args[0];
        String status = (String) map.get("status");
        //一旦业务方法执行完成只会，id就会变成agentId ,所以只能在这里先获取id,原因待考证
        String id = (String) map.get("id");
        result = (Map) joinPoint.proceed(args);
        if (!shouldRecord()) {
            return result;
        }
        UserDetail userDetail = (UserDetail) args[1];
        DataFlowDto dataFlowDto = dataFlowService.findById(new ObjectId(id));

        Operation operationType = null;
        switch (status) {
            case "scheduled":
                log.info("启动了任务");
                operationType = Operation.START;
                break;
            case "stopping":
                operationType = Operation.STOP;
                log.info("停止了任务");
                break;
            case "paused":
                operationType = Operation.PAUSE;
                log.info("暂停了任务");
                break;
            case "force stopping":
                operationType = Operation.FORCE_STOP;
                log.info("强制停止了任务");
                break;
            default:

                log.error(" 任务操作状态参数不对status：{}", status);
        }
        userLogService.addUserLog(Modular.MIGRATION, operationType, userDetail, id, dataFlowDto.getName());

        return result;

    }


    /**
     * 因为需要拿到修改前后的任务名称，所以只能用around方法
     */
    @Around("copyPointcut()")
    public Object afterCopyReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        DataFlowDto result = null;
        Object[] args = joinPoint.getArgs();
        result = (DataFlowDto) joinPoint.proceed(args);


        if (shouldRecord()) {
            //获取方法参数值数组
            String id = (String) args[0];
            UserDetail userDetail = (UserDetail) args[1];
            DataFlowDto originDataFlowDto = dataFlowService.findById(new ObjectId(id));

            //动态修改其参数
            //注意，如果调用joinPoint.proceed()方法，则修改的参数值不会生效，必须调用joinPoint.proceed(Object[] args)

            userLogService.addUserLog(Modular.MIGRATION, Operation.COPY, userDetail, id, originDataFlowDto.getName(), result.getName(), false);
        }


        return result;
    }

  /*  @AfterReturning(value = "copyPointcut()", returning = "result")
    public void afterCopyReturning(Object result) {
        if (!shouldRecord()){
            return  ;
        }

        try {
            DataFlowDto dataFlowDto = (DataFlowDto) result;
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(dataFlowDto.getUserId()));
            userLogService.addUserLog(Modular.MIGRATION, OperationType.COPY, userDetail, dataFlowDto.getId().toString(), dataFlowDto.getName());
        } catch (Exception e) {
            log.error("保持patch操作日志异常", e);
        }
    }*/

    /**
     * 批量启动或者停止任务
     *
     * @return
     */
    @After("resetDataFlowPointcut()")
    public Object afterResetDataFlow(JoinPoint joinPoint) {
        if (!shouldRecord()) {
            return null;
        }
        Object[] args = joinPoint.getArgs();

        DataFlowResetAllReqDto dataFlowResetAllReqDto = (DataFlowResetAllReqDto) args[0];
        UserDetail userDetail = (UserDetail) args[1];

        List<String> ids = dataFlowResetAllReqDto.getId();

        List<ObjectId> objectIdList = ids.stream().map(ObjectId::new).collect(Collectors.toList());

        Operation operationType = null;

        Query query = Query.query(Criteria.where("_id").in(objectIdList));
        List<DataFlow> dataFlowList = dataFlowService.findAll(query, userDetail);

        if (CollectionUtils.isNotEmpty(dataFlowList)) {
            if (dataFlowList.size() > 1) {
                operationType = Operation.BATCH_RESET;

            } else {
                operationType = Operation.RESET;
            }

            for (DataFlow dataFlow : dataFlowList) {
                userLogService.addUserLog(Modular.MIGRATION, operationType, userDetail, dataFlow.getId().toString(), dataFlow.getName());
            }
/*            List<String> names = dataFlowList.stream().map(DataFlow::getName).collect(Collectors.toList());
            List<ObjectId> dataFlowObjectIds = dataFlowList.stream().map(DataFlow::getId).collect(Collectors.toList());

            String resetNames = StringUtils.join(names, ",");
            String resetIds = StringUtils.join(CollectionsUtils.ObjectIdToString(dataFlowObjectIds), ",");
            userLogService.addUserLog(Modular.MIGRATION, operationType, userDetail, resetIds, resetNames);*/
        }

        return null;
    }


    /**
     * 企业版 任务启动或者停止
     *
     * @return
     */
  /*  @After("updateByWherePointcut()")
    public Object afterUpdateByWherePointcut(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        Where where = (Where) args[0];
        Document document = (Document) args[1];
        UserDetail userDetail = (UserDetail) args[2];
        Document setDocument = document.get("$set", Document.class);


        log.info("afterUpdateByWherePointcut id:{}, setDocument:{} ", where.get("_id").toString(), JsonUtil.toJson(setDocument));
        if (null != setDocument) {
            String status = setDocument.getString("status");
            if (StringUtils.isNotEmpty(status)) {
                String id = where.get("_id").toString();
                log.info("dataFlow add message id:{}, status:{}",id, status);
                DataFlowDto dataFlowDto = dataFlowService.findById(MongoUtils.toObjectId(id));
                //add message
                addMessage(id, dataFlowDto.getName(), status, userDetail);
            }
        }
        return null;
    }*/


    /**
     * todo 设置  删除任务  方法为后置通知  目前还只能删除单个任务  因为parseWhere 有问题，这个理还有问题
     */
    @Around("removeDataFlowPointCut()")
    public DataFlowResetAllResDto afterRemoveDataFlow(ProceedingJoinPoint joinPoint) throws Throwable {
        DataFlowResetAllResDto result = null;
        Object[] args = joinPoint.getArgs();
        String whereJson = (String) args[0];
        Where where = parseWhere(whereJson);
        UserDetail userDetail = (UserDetail) args[1];

        List<DataFlowDto> dataFlowDtoList = dataFlowService.findAll(where, userDetail);


        if (CollectionUtils.isNotEmpty(dataFlowDtoList)) {
            List<String> nameList = dataFlowDtoList.stream().map(DataFlowDto::getName).collect(Collectors.toList());
            String names = StringUtils.join(nameList, ",");

            List<ObjectId> idList = dataFlowDtoList.stream().map(DataFlowDto::getId).collect(Collectors.toList());
            String ids = StringUtils.join(idList, ",");

            userLogService.addUserLog(Modular.MIGRATION, Operation.DELETE, userDetail, ids, names);
        }
        result = (DataFlowResetAllResDto) joinPoint.proceed(args);


        return result;
    }


    /**
     * 企业版  此处需要记录flowengine的请求
     *
     * @return
     */
    private Boolean shouldRecord() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();
        if (null == request) {
            return false;
        }
        String userAgent = request.getHeader("User-Agent");
        if (StringUtils.isNotEmpty(userAgent) && (userAgent.contains("Java") || userAgent.contains("java") || userAgent.contains("nodejs"))) {
            log.info("是来自 flowEngine的请求 ");
            return false;
        }
        return true;
    }

    private Where parseWhere(String whereJson) {
        replaceLoopBack(whereJson);
        return JsonUtil.parseJson(whereJson, Where.class);
    }

    private String replaceLoopBack(String json) {
        if (com.tapdata.manager.common.utils.StringUtils.isNotBlank(json)) {
            json = json.replace("\"like\"", "\"$regex\"");
            json = json.replace("\"options\"", "\"$options\"");
            json = json.replace("\"$inq\"", "\"$in\"");
            json = json.replace("\"in\"", "\"$in\"");
        }
        return json;
    }


  /*  private void addMessage(String id, String name, String status, UserDetail userDetail) {
        log.info("dataflow addMessage ,id :{},name :{},status:{}  ", id, name,status);
        if ("scheduled".equals(status))  //  scheduled对应startTime和scheduledTime
        {
            messageService.addMigration(name, id, MsgTypeEnum.STARTED, MsgTypeEnum.STARTED.getValue(), Level.INFO, userDetail);
        } else if ("stopping".equals(status))  // stopping对应stoppingTime
        {
            messageService.addMigration(name, id, MsgTypeEnum.ERROR, MsgTypeEnum.ERROR.getValue(), Level.ERROR, userDetail);
        } else if ("force stopping".equals(status))  // stopping对应stoppingTime
        {

        } else if ("running".equals(status)) {
            messageService.addMigration(name, id, MsgTypeEnum.STARTED, MsgTypeEnum.STARTED.getValue(), Level.INFO, userDetail);
        } else if ("error".equals(status))  // stopping对应stoppingTime
        {
            messageService.addMigration(name, id, MsgTypeEnum.ERROR, MsgTypeEnum.ERROR.getValue(), Level.ERROR, userDetail);
        } else if ("paused".equals(status))  // stopping对应stoppingTime
        {
            messageService.addMigration(name, id, MsgTypeEnum.PAUSED, MsgTypeEnum.PAUSED.getValue(), Level.INFO, userDetail);
        }
    }*/

}
