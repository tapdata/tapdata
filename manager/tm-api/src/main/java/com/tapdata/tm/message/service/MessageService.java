package com.tapdata.tm.message.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.message.vo.MessageListVo;
import org.bson.types.ObjectId;
import org.springframework.scheduling.annotation.Async;

import java.util.Date;
import java.util.List;
import java.util.Locale;

public interface MessageService extends IBaseService<MessageDto,MessageEntity, ObjectId, MessageRepository> {
    Page<MessageListVo> findMessage(Locale locale, Filter filter, UserDetail userDetail);

    Page<MessageListVo> list(Locale locale, MsgTypeEnum type, String level, Boolean read, Integer page, Integer size, UserDetail userDetail);

    long count(Where where, UserDetail userDetail);

    void add(String jobName, String serverName, MsgTypeEnum msgTypeEnum, SystemEnum systemEnum, String sourceId, String title, Level level, UserDetail userDetail);

    @Async("NotificationExecutor")
    void addMigration(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail);

    MessageDto addTrustAgentMessage(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, UserDetail userDetail);

    void addInspect(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail);

    void addSync(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail);

    void addMessage(MessageEntity messageEntity, UserDetail userDetail);

    MessageDto add(MessageDto messageDto, UserDetail userDetail);

    void checkAagentConnectedMessage(Date date, Long agentCount, String address);

    Boolean read(List<String> ids, UserDetail userDetail, Locale locale);

    Boolean readAll(UserDetail userDetail, Locale locale);

    Boolean delete(List<String> idList, UserDetail userDetail, Locale locale);

    Boolean deleteByUserId(UserDetail userDetail, Locale locale);

    MessageDto findById(String id);

    Long checkMessageLimit(UserDetail userDetail);

    boolean checkSending(UserDetail userDetail);
}
