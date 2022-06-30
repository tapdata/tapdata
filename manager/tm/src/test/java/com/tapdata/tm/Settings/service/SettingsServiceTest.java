package com.tapdata.tm.Settings.service;

import com.tapdata.tm.BaseJunit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SettingsServiceTest extends BaseJunit {

    @Autowired
    SettingsService settingsService;

    @Test
    void getByCategoryAndKey() {
    }


    @Test
    void getById() {
       printResult(settingsService.getById("76"));
    }

    @Test
    void findALl() {
    }

}