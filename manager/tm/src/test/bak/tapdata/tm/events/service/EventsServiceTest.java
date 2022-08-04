package com.tapdata.tm.events.service;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.events.dto.EventsDto;
import com.tapdata.tm.events.entity.Events;
import com.tapdata.tm.events.repository.EventsRepository;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.Aggregation;

import java.util.Date;

class EventsServiceTest extends BaseJunit {

    @Autowired
    EventsService eventsService;

    @Autowired
    EventsRepository eventsRepository;


    @Test
    void beforeSave() {
    }

    @Test
    public void findById() {
//        Events events = eventsRepository.findById("618cbb69805a793af36e9cea").get();

        EventsDto eventsDto=eventsService.findById(MongoUtils.toObjectId("618cbb69805a793af36e9cea"));
        printResult(eventsDto);
    }

    @Test
    public void completeInform() {
        eventsService.completeInform();
    }

    @Test
    public void groupBytest() {
//     eventsService.incRetryTime(MongoUtils.toObjectId("61865e055053835e4a43f07b"),getUser("61407a8cfa67f20019f68f9f"));
    }


}
