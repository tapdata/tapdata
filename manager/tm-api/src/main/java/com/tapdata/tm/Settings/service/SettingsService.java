package com.tapdata.tm.Settings.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.dto.TestMailDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

public interface SettingsService {
    @Deprecated
    Object getByCategoryAndKey(String category, String key);

    Object getValueByCategoryAndKey(CategoryEnum category, KeyEnum key);

    boolean isCloud();

    MailAccountDto getMailAccount(String userId);

    Settings getByCategoryAndKey(CategoryEnum category, KeyEnum key);

    Settings getByKey(String key);

    Settings getById(String id);

    List<SettingsDto> findALl(String decode, Filter filter);

    List<SettingsDto> findALl(String decode, Query filter);

    Settings findById(String id);

    long updateByWhere(Where where, Document body);

    long enterpriseUpdate(Where where, String value);

    void save(List<SettingsDto> settingsDto);

    List<Object> getByKeysOnSameCateGory(String category, String... keys);

    void update(SettingsEnum settingsEnum, Object value);

    List<Settings> findAll();

    List<Settings> findAll(Query query);

    void testSendMail(TestMailDto testMailDto);

    void setSettingsRepository(com.tapdata.tm.Settings.repository.SettingsRepository settingsRepository);

    void setMongoTemplate(org.springframework.data.mongodb.core.MongoTemplate mongoTemplate);
}
