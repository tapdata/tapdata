package com.tapdata.tm.Settings.repository;

import com.tapdata.tm.TMApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.jupiter.api.Assertions.*;


@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TMApplication.class})
class SettingsRepositoryTest {

    @Autowired
    SettingsRepository settingsRepository;

    @Test
    void findByCategory() {
//        System.out.println(settingsRepository.findByCategory("Worker"));
    }

    @Test
    void findByCategoryAndKey() {
        System.out.println(settingsRepository.findByCategoryAndKey("Worker","lastHeartbeat"));
    }

}