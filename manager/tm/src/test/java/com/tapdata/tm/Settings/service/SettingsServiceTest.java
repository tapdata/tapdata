package com.tapdata.tm.Settings.service;

import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.repository.SettingsRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SettingsServiceTest {

    @Mock
    private SettingsRepository mockSettingsRepository;
    @Mock
    private MongoTemplate mockMongoTemplate;
    @Mock
    private UserService mockUserService;

    private SettingsService settingsServiceUnderTest;

    @BeforeEach
    void setUp() {
        settingsServiceUnderTest = new SettingsService();
        settingsServiceUnderTest.setSettingsRepository(mockSettingsRepository);
        settingsServiceUnderTest.setMongoTemplate(mockMongoTemplate);
        settingsServiceUnderTest.setUserService(mockUserService);
    }
    @Test
    void testFindALl_isDFS() {
        final Filter filter = new Filter();
        final Settings settings = new Settings();
        settings.setKey("buildProfile");
        settings.setValue("CLOUD");
        List<Settings> list = new ArrayList<>();
        list.add(settings);
        when(mockMongoTemplate.find(any(Query.class), eq(Settings.class))).thenReturn(list);
        final List<SettingsDto> result = settingsServiceUnderTest.findALl("decode", filter);
        assertThat(result.get(0).getValue()).isEqualTo(settings.getValue());
    }

    @Test
    void testFindALl_isDASS() {
        final Filter filter = new Filter();
        final Settings settings = new Settings();
        settings.setKey("buildProfile");
        settings.setValue("DASS");
        List<Settings> list = new ArrayList<>();
        list.add(settings);
        when(mockMongoTemplate.find(any(Query.class), eq(Settings.class))).thenReturn(list);
        when(mockSettingsRepository.findAll()).thenReturn(list);
        final List<SettingsDto> result = settingsServiceUnderTest.findALl("decode", filter);
        assertThat(result.get(0).getValue()).isEqualTo(settings.getValue());
    }
}
