package com.tapdata.tm.license.init;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.license.dto.LicenseDto;
import com.tapdata.tm.license.service.LicenseService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 定时执行，检查license
 */
@Slf4j
//@Component
@Setter(onMethod_ = {@Autowired})
public class LicenseRunner implements ApplicationRunner {
    private SettingsService settingsService;
    private LicenseService licenseService;

    private static Map<String, Object> licenseMap;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        /**
         * 1、生成sid，保存为全局变量
         * 2、如果不存在对应hostname则插入sid、license到mongo，where： hostname
         * 3、启动定时任务，定时检查license
         */
        String sid = licenseService.getSid();
        LicenseDto licenseDto = new LicenseDto();
        licenseDto.setHostname(InetAddress.getLocalHost().getHostName());
        licenseDto.setSid(sid);
        licenseDto.setLicense("");
        Query query = new Query(Criteria.where("hostname").is(licenseDto.getHostname()));
        LicenseDto dbLicenseDto = licenseService.findOne(query);
        if (dbLicenseDto == null) {
            licenseService.upsert(query, licenseDto);
        }
        Settings checkLicense = settingsService.getByKey("checkLicense");
        if (checkLicense != null && "false".equals(checkLicense.getValue())) {
            log.info("Don't check license");
        } else {
            //定时检查license
            new ScheduledThreadPoolExecutor(1)
                    .scheduleAtFixedRate(this::schedule, 0, 1, TimeUnit.MINUTES);
        }

    }

    /**
     * 检查License，将结果放到全局变量中
     */
    private void schedule() {
        try {
            Map<String, Object> resultMap = licenseService.checkLicense();

            String status = (String) resultMap.get("status");
            Long expiresOn = (Long) resultMap.get("expires_on");
            switch (status) {
                case "valid":
                    if (log.isDebugEnabled()) {
                        log.debug("License verification is valid.");
                    }
                    break;
                case "none":
                    long firstStartTime = licenseService.getFirstStartTime();
                    if (!licenseService.checkFreeTime(firstStartTime)) {
                        log.error("More than the free trial period");
                        System.exit(1);
                    }
                    resultMap.put("expires_on", firstStartTime + 1209600000);
                    break;
                case "expired":
                    //失效
                    if (expiresOn != null && !licenseService.checkFreeTime(expiresOn)) {
                        log.error("License expired 60 days");
                        System.exit(1);
                    }
                    break;
                case "invalid_sid":
                    log.error("License cannot be used on this computer");
                    System.exit(1);
                default:
                    log.error("check license failed");
                    System.exit(1);
            }

            licenseMap = resultMap;
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.error("checkLicense error", e);
            } else {
                log.error("checkLicense error {}", e.getMessage());
            }
            System.exit(1);
        }
    }

    public static Map<String, Object> getLicenseMap() {
        return licenseMap;
    }
}
