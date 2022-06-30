package com.tapdata.tm.schedule;

import com.tapdata.tm.BaseJunit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

class InformUserScheduleTest extends BaseJunit {

    @Autowired
    InformUserSchedule informUserSchedule;

    @Test
    void execute() {
        informUserSchedule.execute();
    }
}