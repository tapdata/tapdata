package com.tapdata.tm.events.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.dto.EventsDto;
import com.tapdata.tm.events.entity.Events;
import com.tapdata.tm.events.repository.EventsRepository;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.utils.SendStatus;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;

public abstract class EventsService extends BaseService<EventsDto, Events, ObjectId, EventsRepository> {
    public EventsService(@NonNull EventsRepository repository) {
        super(repository, EventsDto.class, Events.class);
    }
    public abstract List<EventsDto> findByGroupId(String groupId);

    public abstract Events save(Events events);

    public abstract Events recordEvents(String title, String content, String to, String messageId, String userId, SendStatus sendStatus, Integer retry, String eventType);

    public abstract Events asynAdd(String title, String content, String to, String messageId, String userId, String type);

    public abstract Events recordEvents(String title, String content, String to, MessageDto messageDto, SendStatus sendStatus, Integer retry, String eventType);

    public abstract void completeInform();

    public abstract Long updateEventStatus(ObjectId id, String eventStatus, UserDetail userDetail);
}
