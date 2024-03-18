package com.tapdata.tm.message.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.message.vo.MessageListVo;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.scheduling.annotation.Async;

import java.util.Date;
import java.util.List;
import java.util.Locale;

public abstract class MessageService extends BaseService<MessageDto,MessageEntity, ObjectId, MessageRepository> {
    public MessageService(@NonNull MessageRepository repository) {
        super(repository, MessageDto.class, MessageEntity.class);
    }
    public abstract Page<MessageListVo> findMessage(Locale locale, Filter filter, UserDetail userDetail);

    public abstract Page<MessageListVo> list(Locale locale, MsgTypeEnum type, String level, Boolean read, Integer page, Integer size, UserDetail userDetail);

    public abstract long count(Where where, UserDetail userDetail);

    public abstract void add(String jobName, String serverName, MsgTypeEnum msgTypeEnum, SystemEnum systemEnum, String sourceId, String title, Level level, UserDetail userDetail);

    @Async("NotificationExecutor")
    public abstract void addMigration(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail);

    public abstract MessageDto addTrustAgentMessage(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, UserDetail userDetail);

    public abstract void addInspect(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail);

    public abstract void addSync(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail);

    public abstract void addMessage(MessageEntity messageEntity, UserDetail userDetail);

    public abstract MessageDto add(MessageDto messageDto, UserDetail userDetail);

    public abstract void checkAagentConnectedMessage(Date date, Long agentCount, String address);

    public abstract Boolean read(List<String> ids, UserDetail userDetail, Locale locale);

    public abstract Boolean readAll(UserDetail userDetail, Locale locale);

    public abstract Boolean delete(List<String> idList, UserDetail userDetail, Locale locale);

    public abstract Boolean deleteByUserId(UserDetail userDetail, Locale locale);

    public abstract MessageDto findById(String id);

    public abstract Long checkMessageLimit(UserDetail userDetail);

    public abstract boolean checkSending(UserDetail userDetail);
}
