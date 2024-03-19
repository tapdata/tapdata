package com.tapdata.tm.inspect.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.extra.cglib.CglibUtil;
import com.google.common.collect.Maps;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.AlarmSettingTypeEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.inspect.bean.*;
import com.tapdata.tm.inspect.constant.InspectMethod;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.constant.Mode;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.repository.InspectRepository;
import com.tapdata.tm.inspect.vo.InspectDetailVo;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.SyncType;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.*;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class InspectServiceImpl extends InspectService {
    public InspectServiceImpl(@NonNull InspectRepository repository) {
        super(repository);
    }

    @Override
    protected void beforeSave(InspectDto dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Map<String, Long> delete(String id, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<InspectDto> list(Filter filter, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto findById(Filter filter, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void saveInspect(TaskDto taskDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto createCheckByTask(TaskDto taskDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByTaskIdList(List<String> taskIdList) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public UpdateResult deleteByTaskId(String taskId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto updateInspectByWhere(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto executeInspect(Where where, InspectDto updateDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectDto updateById(ObjectId objectId, InspectDto inspectDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByName(String name) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void importData(String json, String upsert, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void setRepeatInspectTask() {
    }

    @Override
    public UpdateResult updateStatusById(String id, InspectStatusEnum inspectStatusEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public UpdateResult updateStatusByIds(List<ObjectId> idList, InspectStatusEnum status) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Map<String, Integer> inspectPreview(UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByStatus(InspectStatusEnum inspectStatusEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findByResult(boolean passed) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void cleanDeadInspect() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void supplementAlarm(InspectDto inspectDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<InspectDto> findAllByIds(List<String> inspectIds) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
