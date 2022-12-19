package com.tapdata.tm.Settings.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.mail.MailUtil;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.dto.TestMailDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.repository.SettingsRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.utils.EncrptAndDencryUtil;

import java.util.*;
import java.util.stream.Collectors;

import com.tapdata.tm.utils.MailUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

/**
 * setting 表的id是字符串而不是ObjectId 导致不能继承baseservece
 */
@Slf4j
@Service
@Setter(onMethod_ = {@Autowired})
public class SettingsService {
    private SettingsRepository settingsRepository;
    private MongoTemplate mongoTemplate;

    /**
     * 有value 则返回value, 没有则返回default_value
     *
     * @param category
     * @param key
     * @return
     */
    @Deprecated
    public Object getByCategoryAndKey(String category, String key) {
        Object value = null;
        Query query = Query.query(Criteria.where("category").is(category));
        query.addCriteria(Criteria.where("key").is(key));

        Settings settings = null;
        List<Settings> settingsList = mongoTemplate.find(query, Settings.class);
        if (CollectionUtils.isNotEmpty(settingsList)) {
            if (settingsList.size() > 1) {
                log.error("数据有误 同样的配置有多条数据，category:{}, key :{} ", category, key);
            }
            settings = settingsList.get(0);

        }

        if (null != settings) {
            value = (null == settings.getValue() ? settings.getDefault_value() : settings.getValue());
        }
        return value;

    }

    public Object getValueByCategoryAndKey(CategoryEnum category, KeyEnum key) {
        Object value = null;
        Settings settings = getByCategoryAndKey(category, key);
        if (settings != null) {
            value = settings.getValue() != null ? settings.getValue() : settings.getDefault_value();
        }

        return value;
    }

    public Settings getByCategoryAndKey(CategoryEnum category, KeyEnum key) {
        Query query = Query.query(Criteria.where("category").is(category.getValue()));
        query.addCriteria(Criteria.where("key").is(key.getValue()));

        Settings settings = null;
        List<Settings> settingsList = mongoTemplate.find(query, Settings.class);
        if (CollectionUtils.isNotEmpty(settingsList)) {
            if (settingsList.size() > 1) {
                log.error("数据有误 同样的配置有多条数据，category:{}, key :{} ", category, key.getValue());
            }
            settings = settingsList.get(0);
        }
        return settings;
    }

    public Settings getByKey(String key) {
        Query query = Query.query(Criteria.where("key").is(key));

        Settings settings = null;
        List<Settings> settingsList = mongoTemplate.find(query, Settings.class);
        if (CollectionUtils.isNotEmpty(settingsList)) {
            if (settingsList.size() > 1) {
                log.error("数据有误 同样的配置有多条数据 key :{} ", key);
            }
            settings = settingsList.get(0);
        }
        return settings;
    }


    public Settings getById(String id) {
        Settings record = (Settings) settingsRepository.findById(id).get();
        return record;

    }

    /**
     * 如果decode等于1
     * {key:"smtp.server.password"}  的value需要加密返回，否则就返回*
     *
     * @param decode
     * @return
     */
    public List<SettingsDto> findALl(String decode, Filter filter) {
        List<Settings> settingsList = new ArrayList<>();
        Where where = filter.getWhere();
        if (null != where.get("id")) {
            String id = String.valueOf(where.get("id"));
            Settings settings = mongoTemplate.findOne(Query.query(Criteria.where("id").is(id)), Settings.class);
            settingsList.add(settings);
        } else {
            settingsList = settingsRepository.findAll();
        }

        if ("1".equals(decode)) {
            //todo 解密方法
            //settingsList.stream().filter(settings -> {
            //    if ("smtp.server.password".equals(settings.getKey())) {
            //        settings.setValue(EncrptAndDencryUtil.Decrypt(String.valueOf(settings.getValue())));
            //        return true;
            //    }
            //    return false;
            //}).collect(Collectors.toList());
        } else {
            settingsList.stream().filter(settings -> {
                if ("smtp.server.password".equals(settings.getKey()))
                    settings.setValue("*****");
                return true;
            }).collect(Collectors.toList());
        }

        List<SettingsDto> settingsDtoList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(settingsList)) {
            settingsList.forEach(settings -> {
                SettingsDto settingsDto = BeanUtil.copyProperties(settings, SettingsDto.class);
                settingsDtoList.add(settingsDto);
            });
        }
        return settingsDtoList;
    }

    public Settings findById(String id) {
        return settingsRepository.findById(id).orElse(null);
    }

    public long updateByWhere(Where where, Document body) {
        Document doc = new Document(where);
        return mongoTemplate.updateFirst(Query.query(Criteria.matchingDocumentStructure(() -> doc)), Update.fromDocument(body), "Settings").getModifiedCount();
    }

    public long enterpriseUpdate(Where where, String value) {
        String id = (String) where.get("_id");
        Query query = Query.query(Criteria.where("id").is(id));
        Update update = new Update();
        update.set("value", value);
        UpdateResult updateResult = mongoTemplate.updateFirst(query, update, Settings.class);
        return updateResult.getModifiedCount();
    }

    public void save(List<SettingsDto> settingsDto) {
        if (CollectionUtils.isNotEmpty(settingsDto)) {
            for (SettingsDto dto : settingsDto) {
                // category: "SMTP", key: "smtp.server.password", value: "*****"
                if (!Objects.isNull(dto.getValue()) &&
                        StringUtils.equals("smtp.server.password", dto.getKey()) &&
                        StringUtils.equals("*****", dto.getValue().toString())) {
                    continue;
                }

                mongoTemplate.save(dto, "Settings");
            }
        }
    }

    /**
     * 相同的categroy 不同的key统一的获取方式
     *
     * @param category
     * @param keys
     * @return
     */
    public List<Object> getByKeysOnSameCateGory(String category, String... keys) {


        Query query = Query.query(Criteria.where("category").is(category).and("key").in(keys));

        List<Settings> settingsList = mongoTemplate.find(query, Settings.class);
        Map<String, List<Settings>> settingMap = settingsList.stream().collect(Collectors.groupingBy(Settings::getKey));
        List<Object> returnObjs = new ArrayList<>();
        for (String key : keys) {
            Settings settings = null;
            Object value = null;
            List<Settings> settings1 = settingMap.get(key);
            if (CollectionUtils.isNotEmpty(settings1)) {
                if (settings1.size() > 1) {
                    log.error("数据有误 同样的配置有多条数据，category:{}, key :{} ", category, key);
                }
                settings = settings1.get(0);
            }
            if (null != settings) {
                value = (null == settings.getValue() ? settings.getDefault_value() : settings.getValue());
            }
            returnObjs.add(value);
        }

        return returnObjs;
    }

    public void update(SettingsEnum settingsEnum, Object value) {
        Criteria criteria = Criteria.where("category").is(settingsEnum.getCategory()).and("key").is(settingsEnum.getKey());
        Query query = new Query(criteria);
        Update update = Update.update("value", value);
        mongoTemplate.updateFirst(query, update, Settings.class);
    }

    public List<Settings> findAll() {
        return mongoTemplate.find(new Query(), Settings.class);
    }

    public void testSendMail(TestMailDto testMailDto) {
        MailAccountDto mailAccount = getMailAccount(testMailDto);

        if ("*****".equals(mailAccount.getPass())) {
            String value = SettingsEnum.SMTP_PASSWORD.getValue();
            mailAccount.setPass(value);
        }

        MailUtils.sendHtmlEmail(mailAccount, mailAccount.getReceivers(), testMailDto.getTitle(), testMailDto.getText());

    }

    private MailAccountDto getMailAccount(TestMailDto testMailDto) {

        String[] split = testMailDto.getEmail_Receivers().split(",");
        return MailAccountDto.builder().host(testMailDto.getSMTP_Server_Host()).port(Integer.valueOf(testMailDto.getSMTP_Server_Port()))
                .from(testMailDto.getEmail_Send_Address()).user(testMailDto.getSMTP_Server_User()).pass(testMailDto.getSMTP_Server_password())
                .receivers(Arrays.asList(split)).protocol(testMailDto.getEmail_Communication_Protocol()).build();
    }
}
