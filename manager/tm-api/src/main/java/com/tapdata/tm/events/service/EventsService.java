package com.tapdata.tm.events.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.dto.EventsDto;
import com.tapdata.tm.events.entity.Events;
import com.tapdata.tm.events.repository.EventsRepository;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.utils.SendStatus;
import org.bson.types.ObjectId;

import java.util.List;

public interface EventsService extends IBaseService<EventsDto, Events, ObjectId, EventsRepository> {
    List<EventsDto> findByGroupId(String groupId);

    Events save(Events events);

    Events recordEvents(String title, String content, String to, String messageId, String userId, SendStatus sendStatus, Integer retry, String eventType);

    Events asynAdd(String title, String content, String to, String messageId, String userId, String type);

    Events recordEvents(String title, String content, String to, MessageDto messageDto, SendStatus sendStatus, Integer retry, String eventType);

    void completeInform();

    Long updateEventStatus(ObjectId id, String eventStatus, UserDetail userDetail);
}
