package com.tapdata.tm.userLog.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.constant.UserLogType;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.dto.User;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.repository.UserLogRepository;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class UserLogService extends BaseService {
    @Autowired
    private UserLogRepository userLogRepository;
    @Autowired
    UserService userService;

    @Autowired
    MessageSource messageSource;

    private final String DESC_PREFIX = "desc.";


    public UserLogService(@NonNull UserLogRepository repository) {
        super(repository, UserLogDto.class, UserLogs.class);
    }


    /**
     * 根据日期筛选，可以生成的方法来做
     * @param filterStr
     * @param userDetail
     * @return
     */
  /*  public Page<UserLogDto> find(String filterStr, UserDetail userDetail) {
        Filter filter = JSONUtil.toBean(filterStr, Filter.class);
        Query query = new Query();
        TmPageable tmPageable = new TmPageable();

        //page由limit 和skip计算的来
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());
        Sort sort = Sort.by("createTime").descending();
        tmPageable.setSort(sort);


        Criteria criteria = Criteria.where("userId").is(userDetail.getUserId());
        Where where = filter.getWhere();
        Long start = (Long) where.get("start");
        Long end = (Long) where.get("end");
        List<Criteria> dateFilter=new ArrayList<>();
        if (null != start) {
            Date startDate = new Date(start);
            dateFilter.add(Criteria.where("createTime").gte(startDate) );
        }

        if (null != end) {
            Date endDate = new Date(end);
            dateFilter.add(  Criteria.where("createTime").lte(endDate));
        }
        if (CollectionUtils.isNotEmpty(dateFilter)){
            criteria.andOperator(dateFilter);
        }
        query.addCriteria(criteria);
        Long total = userLogRepository.getMongoOperations().count(query, UserLogs.class);
        List records = userLogRepository.getMongoOperations().find(query.with(tmPageable), UserLogs.class);

        List<UserLogDto> userLogDtoList=new ArrayList<>();

        if (CollectionUtils.isNotEmpty(records)){
            records.forEach(userLog->{
                UserLogDto userLogDto=BeanUtil.copyProperties(userLog,UserLogDto.class);
                userLogDtoList.add(userLogDto);
            });
        }

        Page<UserLogDto> result = new Page(total, userLogDtoList);
        return result;
    }*/

    /**
     * operationTypeOptions: [
     * // 连接
     * { label: '创建连接', value: 'connection_create', desc: '创建了连接【@{parameter1}】' },
     * { label: '编辑连接', value: 'connection_update', desc: '编辑了连接【@{parameter1}】的配置信息' },
     * { label: '复制连接', value: 'connection_copy', desc: '复制了连接[${parameter1}]为【@{parameter2}】' },
     * { label: '删除连接', value: 'connection_delete', desc: '删除了连接【${parameter1}】' },
     * // 任务
     * { label: '创建任务', value: 'migration_create', desc: '创建了任务【@{parameter1}】' },
     * { label: '启动任务', value: 'migration_start', desc: '启动了任务【@{parameter1}】' },
     * { label: '编辑任务', value: 'migration_update', desc: '编辑了任务【@{parameter1}】的配置信息' },
     * { label: '复制任务', value: 'migration_copy', desc: '复制了任务[${parameter2}] 为【@{parameter1}】' },
     * { label: '重置任务', value: 'migration_reset', desc: '重置了任务【@{parameter1}】' },
     * { label: '删除任务', value: 'migration_delete', desc: '删除了任务【${parameter1}】' },
     * { label: '停止任务', value: 'migration_stop', desc: '停止了任务【@{parameter1}】' },
     * { label: '强制停止任务', value: 'migration_forceStop', desc: '强制停止了任务【@{parameter1}】' },
     * // Agent
     * { label: '修改Agent名称', value: 'agent_rename', desc: '将Agent名称[${parameter2}]修改为【@{parameter1}】' },
     * { label: 'Agent升级', value: 'agent_update', desc: '进行了Agent升级' },
     * // 校验
     * { label: '新建数据校验', value: 'inspect_create', desc: '新建了数据校验任务【@{parameter1}】' },
     * { label: '执行数据校验', value: 'inspect_start', desc: '执行数据校验任务【@{parameter1}】' },
     * // { label: '编辑数据校验', value: 'inspect_update', desc: '编辑了数据校验任务【@{parameter1}】' },
     * { label: '删除数据校验', value: 'inspect_delete', desc: '删除了数据校验任务【${parameter1}】' },
     * // 二次校验
     * {
     * label: '执行差异校验',
     * value: 'differenceInspect_start',
     * desc: '对数据校验任务【@{parameter1}】执行了差异校验'
     * },
     * // 通知
     * { label: '已读全部通知', value: 'message_readAll', desc: '设置全部通知为已读' },
     * { label: '删除全部通知', value: 'message_deleteAll', desc: '删除了全部通知' },
     * { label: '标记通知为已读', value: 'message_read', desc: '将选中的通知全部标记为已读' },
     * { label: '删除通知', value: 'message_delete', desc: '将选中的通知全部删除' },
     * { label: '修改通知设置', value: 'userNotification_update', desc: '修改了系统通知设置' }
     * ]
     *
     * @param modular
     * @param userDetail
     * @param sourceId
     * @param type
     */
    private void addUserLog(Modular modular, Operation operation, UserDetail userDetail, ObjectId sourceId, UserLogType type, String parameter1, String parameter2, Boolean rename) {
        try {
            UserLogs userLogs = new UserLogs();
            userLogs.setModular(modular.getValue());
            userLogs.setOperation(operation.getValue());
            userLogs.setUserId(userDetail.getUserId());
            String userName = StringUtils.isEmpty(userDetail.getUsername()) ? userDetail.getEmail() : userDetail.getUsername();
            userLogs.setUsername(userName);
            userLogs.setSourceId(sourceId);
            userLogs.setType(type.getValue());

            userLogs.setParameter1(parameter1);
            userLogs.setParameter2(parameter2);
            /*      userLogs.setParameter3(parameter3);*/

            User user = new User();
            BeanUtil.copyProperties(userDetail, user);
            userLogs.setUser(user);
            userLogs.setCreateAt(new Date());
            userLogs.setLastUpdAt(new Date());
            userLogs.setLastUpdBy(userDetail.getUsername());
            userLogs.setRename(rename);
            userLogRepository.getMongoOperations().insert(userLogs);

        } catch (Exception e) {
            log.error("执行插入操作日志失败", e);
        }
    }

    public void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1, String parameter2, Boolean rename) {
        ObjectId sourceObjectId = new ObjectId(sourceId);
        addUserLog(modular, OperationType, userDetail, sourceObjectId, UserLogType.USER_OPERATION, parameter1, parameter2, rename);
    }

    public void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1) {
        ObjectId sourceObjectId = new ObjectId(sourceId);
        addUserLog(modular, OperationType, userDetail, sourceObjectId, UserLogType.USER_OPERATION, parameter1, null, false);
    }

    public void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail , String parameter1) {
        addUserLog(modular, OperationType, userDetail, null, UserLogType.USER_OPERATION, parameter1, null, false);
    }


    /**
     * 给当前登录的用户添加操作日志
     *
     * @param modular
     * @param OperationType
     * @param sourceId
     * @param parameter1
     */
    public void addUserLog(Modular modular, Operation OperationType, String userId, String sourceId, String parameter1) {
        ObjectId sourceObjectId =null;
        if (StringUtils.isNotBlank(sourceId)){
            sourceObjectId= new ObjectId(sourceId);
        }
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
        addUserLog(modular, OperationType, userDetail, sourceObjectId, UserLogType.USER_OPERATION, parameter1, null, false);
    }

    public void addUserLog(Modular modular, Operation OperationType, String parameter1, UserDetail userDetail ) {
        addUserLog(modular, OperationType, userDetail, null, UserLogType.USER_OPERATION, parameter1, null, false);
    }



    @Override
    protected void beforeSave(BaseDto dto, UserDetail userDetail) {

    }
}
