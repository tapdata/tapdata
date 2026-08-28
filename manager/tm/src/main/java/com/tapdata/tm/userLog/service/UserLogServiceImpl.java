package com.tapdata.tm.userLog.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserServiceImpl;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.constant.AuditEventType;
import com.tapdata.tm.userLog.constant.AuditOutcome;
import com.tapdata.tm.userLog.constant.UserLogTemplateKey;
import com.tapdata.tm.userLog.constant.UserLogType;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.dto.User;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.param.AuditLogParam;
import com.tapdata.tm.userLog.repository.UserLogRepository;
import com.tapdata.tm.utils.IpUtil;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import jakarta.servlet.http.HttpServletRequest;

import java.util.*;

@Slf4j
@Service
public class UserLogServiceImpl extends BaseService implements UserLogService{
    private static final String UNAUTHENTICATED_USER = "UNAUTHENTICATED";
    private static final List<String> FORWARDED_IP_HEADERS = List.of(
            "Forwarded",
            "X-Forwarded-For",
            "X-Real-IP",
            "Proxy-Client-IP",
            "WL-Proxy-Client-IP",
            "HTTP_CLIENT_IP",
            "HTTP_X_FORWARDED_FOR"
    );

    @Autowired
    private UserLogRepository userLogRepository;
    @Autowired
    UserServiceImpl userService;

    @Autowired
    MessageSource messageSource;

    private final String DESC_PREFIX = "desc.";

    public UserLogServiceImpl(@NonNull UserLogRepository repository) {
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
        addUserLog(modular, operation, userDetail, sourceId, type, parameter1, parameter2, rename, false);
    }

    protected void addUserLog(Modular modular, Operation operation, UserDetail userDetail, ObjectId sourceId, UserLogType type, String parameter1, String parameter2, Boolean rename, boolean systemStart) {
        try {
            UserLogs userLogs = new UserLogs();
            userLogs.setModular(modular.getValue());
            userLogs.setOperation(operation != null ? operation.getValue() : null);
            userLogs.setUserId(userDetail.getUserId());
            String userName = StringUtils.isEmpty(userDetail.getUsername()) ? userDetail.getEmail() : userDetail.getUsername();
            userLogs.setUsername(systemStart ? "SYSTEM" : userName);
            userLogs.setIp(resolveSourceIp());
            userLogs.setSourceId(sourceId);
            userLogs.setType(type.getValue());
            userLogs.setEventType(resolveEventType(modular, operation, type));
            userLogs.setOutcome(AuditOutcome.SUCCESS.getValue());
            userLogs.setEventId(UUID.randomUUID().toString());
            userLogs.setObjectName(safeValue(parameter1));

            userLogs.setParameter1(safeValue(parameter1));
            userLogs.setParameter2(safeValue(parameter2));
            /*      userLogs.setParameter3(parameter3);*/

            User user = new User();
            BeanUtil.copyProperties(userDetail, user);
            userLogs.setUser(user);
            userLogs.setCreateAt(new Date());
            userLogs.setLastUpdAt(new Date());
            userLogs.setLastUpdBy(userDetail.getUsername());
            userLogs.setRename(rename);
            //userLogRepository.getMongoOperations().insert(userLogs);
            userLogRepository.insert(userLogs, userDetail);

        } catch (Exception e) {
            log.error("执行插入操作日志失败", e);
        }
    }

    @Override
    public void addAuditLog(AuditLogParam auditLogParam) {
        if (auditLogParam == null) {
            return;
        }
        try {
            UserLogs userLogs = new UserLogs();
            String userId = StringUtils.isNotBlank(auditLogParam.getUserId())
                    ? auditLogParam.getUserId() : UNAUTHENTICATED_USER;
            String username = StringUtils.isNotBlank(auditLogParam.getUsername())
                    ? auditLogParam.getUsername() : userId;
            userLogs.setUserId(userId);
            userLogs.setCustomId(auditLogParam.getCustomerId());
            userLogs.setUsername(username);
            userLogs.setIp(resolveSourceIp());
            userLogs.setEventType(auditLogParam.getEventType() == null
                    ? AuditEventType.ADMIN_OPERATION.getValue() : auditLogParam.getEventType().getValue());
            userLogs.setOutcome(auditLogParam.getOutcome() == null
                    ? AuditOutcome.FAILURE.getValue() : auditLogParam.getOutcome().getValue());
            userLogs.setOperation(safeValue(auditLogParam.getAction()));
            userLogs.setParameter1(safeValue(auditLogParam.getParameter1()));
            userLogs.setObjectName(safeValue(auditLogParam.getObjectName()));
            userLogs.setFailureReason(safeValue(auditLogParam.getFailureReason()));
            userLogs.setChangeSummary(safeValue(auditLogParam.getChangeSummary()));
            userLogs.setServiceNode(safeValue(auditLogParam.getServiceNode()));
            userLogs.setComponentType(safeValue(auditLogParam.getComponentType()));
            userLogs.setInstanceName(safeValue(auditLogParam.getInstanceName()));
            userLogs.setLoginMethod(safeValue(auditLogParam.getLoginMethod()));
            userLogs.setEventId(UUID.randomUUID().toString());
            userLogs.setType(UserLogType.USER_OPERATION.getValue());
            userLogs.setModular(Modular.SYSTEM.getValue());
            userLogs.setCreateAt(new Date());
            userLogs.setLastUpdAt(new Date());
            userLogs.setLastUpdBy(username);
            userLogRepository.getMongoOperations().insert(userLogs, userLogRepository.getCollectionName());
        } catch (Exception e) {
            log.error("Failed to insert audit log", e);
        }
    }

    @Override
    public UserLogDto findById(String id, UserDetail userDetail) {
        if (!ObjectId.isValid(id)) {
            return null;
        }
        return userLogRepository.findById(new ObjectId(id), userDetail)
                .map(entity -> (UserLogDto) convertToDto(entity, UserLogDto.class, "password"))
                .orElse(null);
    }

    private String resolveSourceIp() {
        try {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attributes == null) {
                return null;
            }
            return resolveSourceIp(attributes.getRequest());
        } catch (Exception e) {
            return null;
        }
    }

    String resolveSourceIp(HttpServletRequest request) {
        if (request == null || FORWARDED_IP_HEADERS.stream()
                .map(request::getHeader)
                .anyMatch(StringUtils::isNotBlank)) {
            return null;
        }
        return normalizeIp(request.getRemoteAddr());
    }

    private String resolveEventType(Modular modular, Operation operation, UserLogType type) {
        if (modular == Modular.SYSTEM && (operation == Operation.LOGIN || operation == Operation.LOGOUT)) {
            return AuditEventType.LOGIN.getValue();
        }
        if (modular == Modular.USER || modular == Modular.ROLE || modular == Modular.ACCESS_CODE) {
            return AuditEventType.ADMIN_OPERATION.getValue();
        }
        if (modular == Modular.AGENT && (operation == Operation.START || operation == Operation.STOP
                || operation == Operation.RESTART_AGENT || operation == Operation.DELETE)) {
            return AuditEventType.SERVICE_LIFECYCLE.getValue();
        }
        return type == UserLogType.USER_OPERATION
                ? AuditEventType.USER_OPERATION.getValue() : type.getValue();
    }

    private String normalizeIp(String ip) {
        String normalized = ip == null ? "" : ip.trim();
        if ("::1".equals(normalized) || "0:0:0:0:0:0:0:1".equals(normalized)) {
            return "127.0.0.1";
        }
        if (normalized.startsWith("::ffff:")) {
            normalized = normalized.substring("::ffff:".length());
        }
        return IpUtil.check(normalized) == null ? null : normalized;
    }

    private String safeValue(String value) {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.contains("password=") || lower.contains("token=") || lower.contains("secret=")
                || lower.contains("private_key") || lower.contains("privatekey") || lower.contains("cookie=")) {
            return "[REDACTED]";
        }
        return value;
    }

    public void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1, String parameter2, Boolean rename) {
        ObjectId sourceObjectId = sourceId != null ? new ObjectId(sourceId) : null;
        addUserLog(modular, OperationType, userDetail, sourceObjectId, UserLogType.USER_OPERATION, parameter1, parameter2, rename);
    }

    public void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1) {
        ObjectId sourceObjectId = sourceId != null ? new ObjectId(sourceId) : null;
        addUserLog(modular, OperationType, userDetail, sourceObjectId, UserLogType.USER_OPERATION, parameter1, null, false);
    }

    public void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1, Object systemStart) {
        ObjectId sourceObjectId = sourceId != null ? new ObjectId(sourceId) : null;
        addUserLog(modular, OperationType, userDetail, sourceObjectId, UserLogType.USER_OPERATION, parameter1, null, false, systemStart instanceof Boolean ? (boolean)systemStart : systemStart != null);
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

    public Page<UserLogDto> find(Filter filter, UserDetail userDetail) {

        if (filter == null) {
            filter = new Filter();
        }
        if (filter.getSort().isEmpty()) {
            filter.setSort(Collections.singletonList("createAt DESC"));
        }

        List<UserLogs> entityList = userLogRepository.findAll(filter, userDetail);

        long total = userLogRepository.count(filter.getWhere(), userDetail);

        List<UserLogDto> items = convertToDto(entityList, UserLogDto.class, "password");
        Locale locale = MessageUtil.getLocale();
        items.forEach(item -> item.setI18nMessage(renderI18nMessage(item, locale)));

        return new Page<>(total, items);
    }

    protected String renderI18nMessage(UserLogDto dto, Locale locale) {
        String modular = valueOrEmpty(dto.getModular());
        String operation = valueOrEmpty(dto.getOperation());
        String specificKey = UserLogTemplateKey.specificOperation(modular, operation);
        String defaultKey = UserLogTemplateKey.defaultOperation(operation);

        String template = MessageUtil.getBundleMessageOrNull(locale, UserLogTemplateKey.BUNDLE_NAME, specificKey);
        if (template == null) {
            template = MessageUtil.getBundleMessageOrNull(locale, UserLogTemplateKey.BUNDLE_NAME, defaultKey);
        }
        if (template == null) {
            log.warn("User log i18n template not found, modular: {}, operation: {}, specificKey: {}, defaultKey: {}",
                    modular, operation, specificKey, defaultKey);
            return fallbackI18nMessage(dto, modular, operation);
        }

        return MessageUtil.formatString(template, buildTemplateParams(locale, modular));
    }

    private Map<String, Object> buildTemplateParams(Locale locale, String modular) {
        Map<String, Object> params = new HashMap<>();
        params.put("moduleName", getModuleName(locale, modular));
        return params;
    }

    private String getModuleName(Locale locale, String modular) {
        String moduleKey = UserLogTemplateKey.moduleName(modular);
        String moduleName = MessageUtil.getBundleMessageOrNull(locale, UserLogTemplateKey.BUNDLE_NAME, moduleKey);
        if (moduleName == null) {
            log.warn("User log i18n module name not found, modular: {}, moduleKey: {}", modular, moduleKey);
            return modular;
        }
        return moduleName;
    }

    private String fallbackI18nMessage(UserLogDto dto, String modular, String operation) {
        return valueOrEmpty(dto.getUsername()) + " " + modular + "." + operation + " " + valueOrEmpty(dto.getParameter1());
    }

    private String valueOrEmpty(String value) {
        return value == null ? "" : value;
    }
}
