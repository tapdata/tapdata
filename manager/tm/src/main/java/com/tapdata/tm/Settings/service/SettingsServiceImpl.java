package com.tapdata.tm.Settings.service;

import cn.hutool.core.bean.BeanUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.dto.TestMailDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.repository.SettingsRepository;
import com.tapdata.tm.Settings.service.util.SettingServiceUtil;
import com.tapdata.tm.alarmMail.dto.AlarmMailDto;
import com.tapdata.tm.alarmMail.service.AlarmMailService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.*;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.caffeine.CaffeineCacheManager;
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
public class SettingsServiceImpl implements SettingsService {
    private SettingsRepository settingsRepository;
    private MongoTemplate mongoTemplate;

    private AlarmMailService alarmMailService;

    @Qualifier("caffeineCache")
    private CaffeineCacheManager caffeineCacheManager;

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

    public boolean isCloud() {
        Object buildProfile = getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
        if (Objects.isNull(buildProfile)) {
            buildProfile = "DAAS";
        }
        return buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");
    }

    public MailAccountDto getMailAccount(String userId) {
        List<Settings> all = findAll();
        Map<String, Object> collect = all.stream().collect(Collectors.toMap(Settings::getKey, Settings::getValue, (e1, e2) -> e1));

        String host = (String) collect.get("smtp.server.host");
        String port = (String) collect.getOrDefault("smtp.server.port", "0");
        String from = (String) collect.get("email.send.address");
        String user = (String) collect.get("smtp.server.user");
        Object pwd = collect.get("smtp.server.password");
        String password = Objects.nonNull(pwd) ? pwd.toString() : null;
        String protocol = (String) collect.get("email.server.tls");
        String proxyHost = (String) collect.get("smtp.proxy.host");
        String proxyPort = (String) collect.get("smtp.proxy.port");
        proxyPort = StringUtils.isNotBlank(proxyPort) ? proxyPort : "0";

        AtomicReference<List<String>> receiverList = new AtomicReference<>(new ArrayList<>());

        boolean isCloud = isCloud();
        if (isCloud) {
            UserService userService = SpringContextHelper.getBean(UserService.class);
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
            Optional.ofNullable(userDetail).ifPresent(u -> {
                if (StringUtils.isNotBlank(u.getEmail())) {
                    receiverList.set(Lists.newArrayList(u.getEmail()));
                }
            });
            AlarmMailDto alarmMailDto = alarmMailService.findOne(new Query(),userDetail);
            if( alarmMailDto != null && CollectionUtils.isNotEmpty(alarmMailDto.getEmailAddressList())){
                receiverList.get().addAll(alarmMailDto.getEmailAddressList());
            }
        } else {
            String receivers = (String) collect.get("email.receivers");
            if (StringUtils.isNotBlank(receivers)) {
                String[] split = receivers.split(",");
                receiverList.set(Arrays.asList(split));
            }
        }

        return MailAccountDto.builder().host(host).port(Integer.valueOf(port)).from(from).user(user).pass(password)
                .receivers(receiverList.get()).protocol(protocol).proxyHost(proxyHost).proxyPort(Integer.valueOf(proxyPort)).build();
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
            if(isCloud()){
                Query query = Query.query(Criteria.where("key").in("buildProfile", "threshold", "logLevel", "job_cdc_record"));
                settingsList = Optional.ofNullable(caffeineCacheManager.getCache("cloudSettings"))
                        .map(cache -> cache.get("cloudSettings", () -> findAll(query)))
                        .orElseGet(() -> findAll(query));

            }else {
                settingsList = settingsRepository.findAll();
            }
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
                if ("smtp.server.password".equals(settings.getKey()) || "ldap.bind.password".equals(settings.getKey()))
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
    public List<SettingsDto> findALl(String decode, Query filter) {
        List<Settings> settingsList = mongoTemplate.find(filter, Settings.class);
        return SettingServiceUtil.copyProperties(decode, settingsList);
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
        return findAll(new Query());
    }

    public List<Settings> findAll(Query query) {
        return mongoTemplate.find(query, Settings.class);
    }

    public TestResponseDto testSendMail(TestMailDto testMailDto) {
        MailAccountDto mailAccount = getMailAccount(testMailDto);

        if ("*****".equals(mailAccount.getPass())) {
            String value = SettingsEnum.SMTP_PASSWORD.getValue();
            mailAccount.setPass(value);
        }

        return MailUtils.sendHtmlEmail(mailAccount, mailAccount.getReceivers(), testMailDto.getTitle(), testMailDto.getText());

    }

    protected MailAccountDto getMailAccount(TestMailDto testMailDto) {
        String port = StringUtils.isNotBlank(testMailDto.getSMTP_Server_Port()) ? testMailDto.getSMTP_Server_Port() : "0";
        String proxyPort = StringUtils.isNotBlank(testMailDto.getSMTP_Proxy_Port()) ? testMailDto.getSMTP_Proxy_Port() : "0";
        String[] split = testMailDto.getEmail_Receivers().split(",");
        return MailAccountDto.builder().host(testMailDto.getSMTP_Server_Host()).port(Integer.valueOf(port))
                .from(testMailDto.getEmail_Send_Address()).user(testMailDto.getSMTP_Server_User()).pass(testMailDto.getSMTP_Server_password())
                .receivers(Arrays.asList(split)).protocol(testMailDto.getEmail_Communication_Protocol())
                .proxyHost(testMailDto.getSMTP_Proxy_Host()).proxyPort(Integer.valueOf(proxyPort)).build();
    }

    public String applicationVersion() {
        String defaultVersion = "DAAS_BUILD_NUMBER";
        //从环境变量读取
        String tapdataVersion = System.getenv("tapdataVersion");
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> map;
        try {
            map = objectMapper.readValue(tapdataVersion != null ? tapdataVersion : "{}", Map.class);
        } catch (IOException e) {
            throw new BizException("read.version.file.failed", e);
        }
        String appVersion = (String) map.get("app_version");
        log.info("app version: {}", appVersion);
        return appVersion != null ? appVersion : defaultVersion;
    }
}
