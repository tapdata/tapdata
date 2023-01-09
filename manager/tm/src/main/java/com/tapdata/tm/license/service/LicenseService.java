package com.tapdata.tm.license.service;

import cn.hutool.core.codec.Base64Decoder;
import cn.hutool.core.codec.Base64Encoder;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.crypto.KeyUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.json.JSONUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.license.dto.LicenseDto;
import com.tapdata.tm.license.dto.LicenseUpdateDto;
import com.tapdata.tm.license.entity.LicenseEntity;
import com.tapdata.tm.license.repository.LicenseRepository;
import com.tapdata.tm.license.util.SidUtil;
import com.tapdata.tm.utils.RC4Util;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.*;

/**
 * @Author:
 * @Date: 2021/12/06
 * @Description:
 */
@Service
@Slf4j
public class LicenseService extends BaseService<LicenseDto, LicenseEntity, ObjectId, LicenseRepository> {

    private static final Path licenseFilePath;
    private static final String cert;
    private static final PublicKey publicKey;
    private static final Path homeDir;
    private static final String RUN_ENV_TYPE = System.getenv("tapdata_running_env");
    private static String sid;

    static {
        String workDir = System.getenv("TAPDATA_WORK_DIR");
        homeDir = Paths.get(System.getProperty("user.home"));
        if (StringUtils.isEmpty(workDir)) {
            workDir = homeDir + File.separator + ".tapdata" + File.separator;
        }
        licenseFilePath = Paths.get(workDir, "license.txt");
        cert = "-----BEGIN CERTIFICATE-----\n" +
                "MIIDazCCAlOgAwIBAgIEDTzOhzANBgkqhkiG9w0BAQsFADBlMQswCQYDVQQGEwJj\n" +
                "bjEPMA0GA1UECAwG5bm/5LicMQ8wDQYDVQQHDAbmt7HlnLMxEDAOBgNVBAoTB3Rh\n" +
                "cGRhdGExEDAOBgNVBAsTB3RhcGRhdGExEDAOBgNVBAMTB3RhcGRhdGEwIBcNMTkw\n" +
                "NjE5MDcyMDI4WhgPMjExOTA1MjYwNzIwMjhaMGUxCzAJBgNVBAYTAmNuMQ8wDQYD\n" +
                "VQQIDAblub/kuJwxDzANBgNVBAcMBua3seWcszEQMA4GA1UEChMHdGFwZGF0YTEQ\n" +
                "MA4GA1UECxMHdGFwZGF0YTEQMA4GA1UEAxMHdGFwZGF0YTCCASIwDQYJKoZIhvcN\n" +
                "AQEBBQADggEPADCCAQoCggEBAMIcwc5l/MKrDok4mca0E7C9k9Ive0JiE67RjpJ2\n" +
                "Zfl8idKaO7Y1cIMsoBkm7p0w+/zPj1etNsd17i/47HkTojpnBR2HwakTVluo6RiY\n" +
                "u4B+xYAxZqGAPVfeo/W9olSa1tYWO5kEQiyaRnHjcWdVmN6d/xj8JFfmBz4sdJQA\n" +
                "CJeXVVmq6SCq7lJMm1eQZZyNs+qmbc8jGmzdoZVkoe2RaQ65H6YV/zGSOx8mzVhC\n" +
                "tyIIhS03Z1re2XZYVs3dRLPyDDI+7aTqCnTOStyT+g4BkqJMlPfqSomoGXR9i5LW\n" +
                "DI7pvH79AAzGititAjeAhD//du+/Nm0OVwVr9fcr7Yz3UnECAwEAAaMhMB8wHQYD\n" +
                "VR0OBBYEFM8oIny0RnOmcwkGW1l+XhbUrVWcMA0GCSqGSIb3DQEBCwUAA4IBAQB2\n" +
                "vi0yH4EADhRDeo3D5XcRh6TdkDu01SSE4sSPrIqU9xCRF/q5fkgkZVsU1aUibU/Z\n" +
                "a23zkLxoyIJMe3o3IEpZvb0v5J/6l77eeXUVEmZPwZf5+Sd3Y3ZgQPjmOjd0+gzJ\n" +
                "GwAskcxFHX8Tow8lYR/t52LinPBidThMJ2dG09K36nbUI7HHVS2gYzUi7+vRsVv6\n" +
                "rGDQzDdpz/KGb9TRFTOshsvDom8xROUcAOx2t8BylcU3BdsEoSPQnaMvbaJaBL6q\n" +
                "dCq5t6EGidBi8XUymh2VEhuWqkwHdzSQAJJxTeLfcUpNJU9cNCtzlt6QIRUYjh0e\n" +
                "UioXhb8JTdted94F8xSZ\n" +
                "-----END CERTIFICATE-----";
        //从证书中获取公钥
        Certificate certificate = KeyUtil.readX509Certificate(IoUtil.toStream(cert.getBytes(StandardCharsets.UTF_8)));
        publicKey = certificate.getPublicKey();
    }

    public LicenseService(@NonNull LicenseRepository repository) {
        super(repository, LicenseDto.class, LicenseEntity.class);
    }

    protected void beforeSave(LicenseDto license, UserDetail user) {

    }

    public boolean checkFreeTime(long firstStartTime) {
        long now = new Date().getTime();
        if (now >= firstStartTime && now < firstStartTime + 1209600000) {
            return true;
        }
        return false;
    }

    public Map<String, Object> checkLicense() {

        try {
            this.syncLicenseFileAndMongodb();
            LicenseDto licenseDto;
            if (StringUtils.equalsAnyIgnoreCase(RUN_ENV_TYPE, "kubernetes")) {
                String hostName = InetAddress.getLocalHost().getHostName();
                Query query = Query.query(Criteria.where("hostname").is(hostName));
                licenseDto = this.findOne(query);
            } else {
                licenseDto = readLicenseFromFile();
            }
            if (licenseDto == null) {
                return new HashMap<String, Object>() {{
                    put("status", "none");
                    put("msg", "No license");
                }};
            }
            String machineSid = getSid();
            //验证license中是否包含机器sid
            if (!checkSid(machineSid, licenseDto)) {
                log.error("Does not match license sid.");
                return new HashMap<String, Object>() {{
                    put("status", "invalid_sid");
                    put("msg", "Does not match license sid");
                    put("sid", machineSid);
                    put("license_sid", licenseDto.getSid());

                }};
            }
            //验证过期时间
            if (licenseDto.getValidity_period() == null || licenseDto.getValidity_period().getExpires_on() == null) {
                log.error("Invalid license, not define \"validity_period\" property in license.");
                return new HashMap<String, Object>() {{
                    put("status", "invalid_sid");
                    put("msg", "Does not match license sid");
                    put("sid", machineSid);
                    put("license_sid", licenseDto.getSid());
                }};
            }

            Long expires_on = licenseDto.getValidity_period().getExpires_on();
            if (expires_on < new Date().getTime()) {
                return new HashMap<String, Object>() {{
                    put("status", "expired");
                    put("msg", "License is expired");
                    put("expires_on", expires_on);
                }};
            }

            //校验通过
            return new HashMap<String, Object>() {{
                put("status", "valid");
                put("msg", "Valid license");
                put("expires_on", expires_on);
            }};


        } catch (Throwable e) {
            log.error("checkLicense error", e);
            return new HashMap<String, Object>() {{
                put("status", "checkLicense error");
                put("msg", e.getMessage());
            }};
        }
    }

    /**
     * 双向同步file和mongodb的license信息
     * @throws Throwable
     */
    private void syncLicenseFileAndMongodb() throws Throwable {

        /**
         *  updateLicense
         *  1、获取sid
         *  2、license文件是否存在
         *  3、根据hostname查询mongo中的license信息
         *  4、如果license文件不存在，则更新文件
         *  5、其他条件处理
         */
        String currentSid = this.getSid();
        LicenseDto fileLicenseDto = this.readLicenseFromFile();
        LicenseDto dbLicenseDto = this.findOne(new Query(Criteria.where("hostname").is(InetAddress.getLocalHost().getHostName())));

        if ((fileLicenseDto == null || StringUtils.isNotEmpty(fileLicenseDto.getLicense()))
                && dbLicenseDto != null && StringUtils.isNotEmpty(dbLicenseDto.getLicense())) {
            //更新到文件
            FileUtil.writeString(dbLicenseDto.getLicense(), licenseFilePath.toFile(), StandardCharsets.UTF_8);
        } else if ((dbLicenseDto == null || StringUtils.isEmpty(dbLicenseDto.getLicense()))
                && fileLicenseDto != null && StringUtils.isNotEmpty(fileLicenseDto.getLicense())) {
            if (checkSid(currentSid, fileLicenseDto)) {
                updateLicenseByHostname(fileLicenseDto, dbLicenseDto);
            }
        } else if (fileLicenseDto != null && dbLicenseDto != null) {
            //对比数据进行更新
            if (StringUtils.equals(dbLicenseDto.getLicense(), fileLicenseDto.getLicense())) {
                //不做处理
            } else {
                LicenseDto mongoLicenseDto = JsonUtil.parseJson(this.decryptLicense(dbLicenseDto.getLicense()), LicenseDto.class);
                if (fileLicenseDto.getValidity_period() != null && fileLicenseDto.getValidity_period().getExpires_on() != null
                        && mongoLicenseDto.getValidity_period() != null && mongoLicenseDto.getValidity_period().getExpires_on() != null) {
                    //比对失效时间，取最新的
                    if (dbLicenseDto.getValidity_period().getExpires_on() > mongoLicenseDto.getValidity_period().getExpires_on()) {
                        if (checkSid(currentSid, fileLicenseDto)) {
                            updateLicenseByHostname(fileLicenseDto, dbLicenseDto);
                        }
                    } else {
                        FileUtil.writeString(dbLicenseDto.getLicense(), licenseFilePath.toFile(), StandardCharsets.UTF_8);
                    }

                } else if (fileLicenseDto.getValidity_period() != null && fileLicenseDto.getValidity_period().getExpires_on() != null
                        && (mongoLicenseDto.getValidity_period() == null || mongoLicenseDto.getValidity_period().getExpires_on() == null)) {
                    if (checkSid(currentSid, fileLicenseDto)) {
                        updateLicenseByHostname(fileLicenseDto, dbLicenseDto);
                    }
                } else if (mongoLicenseDto.getValidity_period() != null && mongoLicenseDto.getValidity_period().getExpires_on() !=null
                        && (fileLicenseDto.getValidity_period() == null || fileLicenseDto.getValidity_period().getExpires_on() == null)) {
                    FileUtil.writeString(dbLicenseDto.getLicense(), licenseFilePath.toFile(), StandardCharsets.UTF_8);
                }
            }
        }
    }

    private void updateLicenseByHostname(LicenseDto fileLicenseDto, LicenseDto dbLicenseDto) {
        if (dbLicenseDto == null) {
            dbLicenseDto = new LicenseDto();
        }
        dbLicenseDto.setLicense(fileLicenseDto.getLicense());
        dbLicenseDto.setExpires_on(fileLicenseDto.getExpires_on());
        dbLicenseDto.setExpirationDate(fileLicenseDto.getExpirationDate());
        this.upsert(new Query(Criteria.where("hostname").is(fileLicenseDto.getHostname())), dbLicenseDto);
    }

    /**
     * 校验license中是否包括指定的sid
     * @param sid
     * @param licenseDto
     * @return
     * @throws Throwable
     */
    public boolean checkSid(String sid, LicenseDto licenseDto) throws Throwable {
        if (!licenseDto.getSid().contains(sid)) {
            String nsid = RC4Util.decrypt("Gotapd8", sid);
            List<String> nsidList = JSONUtil.toList(nsid, String.class);

            String decryptLicense = this.decryptLicense(licenseDto.getLicense());
            LicenseDto parseLicense = JsonUtil.parseJson(decryptLicense, LicenseDto.class);
            String[] licenseSidArray = parseLicense.getSid().split("-");
            for (String licenseSid : licenseSidArray) {
                String decryptLicenseSid = RC4Util.decrypt("Gotapd8", licenseSid);
                List<String> decryptLicenseSidList = JSONUtil.toList(decryptLicenseSid, String.class);
                for (String s : nsidList) {
                    if (decryptLicenseSidList.contains(s)) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            return true;
        }
    }

    public long getFirstStartTime() throws IOException {
        long firstStartTime = 0L;
        Path firstStartFileDir = Paths.get(homeDir.toString(), ".local", "share", ".tapdata");
        if (Files.notExists(firstStartFileDir)) {
            Files.createDirectories(firstStartFileDir);
        }
        Path firstStartTimeFile = Paths.get(firstStartFileDir.toString(), ".tfst");
        if (Files.exists(firstStartTimeFile)) {
            String firstStartTimeStr = String.join("", Files.readAllLines(firstStartTimeFile)).trim();
            firstStartTime = Long.parseLong(firstStartTimeStr);
        }
        if (firstStartTime <= 0) {
            firstStartTime = new Date().getTime();
            FileUtil.writeString(String.valueOf(firstStartTime), firstStartTimeFile.toFile(), StandardCharsets.UTF_8);
        }

        return firstStartTime;
    }

    public List<LicenseDto> getLicensesBySids(List<String> ids) {
        Filter filter = new Filter();
        Where where = new Where();
        Map<String, Object> ex = new HashMap<>();
        ex.put("$in", ids);
        where.put("sid", ex);
        filter.setWhere(where);

        List<LicenseDto> list = this.findAll(filter);
        return list;
    }

    /**
     * 解密license
     * @param encodeLicense
     * @return
     * @throws Throwable
     */
    public String decryptLicense(String encodeLicense) throws Throwable {
        String[] encodeLicenses = encodeLicense.trim().split("\\.");

        String encryptedData = encodeLicenses[0];
        String encryptionKeyData = encodeLicenses[1];

        //key为解密后的字节数组的字符串格式
        String key = SecureUtil.rsa(null, publicKey.getEncoded()).decryptStr(encryptionKeyData, KeyType.PublicKey);

        return Aes.decrypt(encryptedData, key);
    }

    private LicenseDto readLicenseFromFile() throws Throwable {
        try {
            if (Files.exists(licenseFilePath)) {
                String encodeLicense = String.join("", Files.readAllLines(licenseFilePath)).trim();
                String license = decryptLicense(encodeLicense);
                LicenseDto licenseDto = JsonUtil.parseJson(license, LicenseDto.class);
                licenseDto.setLicense(encodeLicense);
                return licenseDto;
            }
        } catch (Throwable e) {
            if (log.isDebugEnabled()) {
                log.error("Read license fail", e);
            } else {
                log.error("Read license fail {}", e.getMessage());
            }
        }
        return null;
    }

    public boolean updateLicense(LicenseUpdateDto updateDto) {
        List<LicenseDto> dbLicenseList = this.getLicensesBySids(updateDto.getReqDto().getSid());
        boolean isUpdate = false;
        for (LicenseDto dbLicense : dbLicenseList) {
            if (StringUtils.isNotEmpty(dbLicense.getSid()) && updateDto.getSid().contains(dbLicense.getSid())) {
                //license中授权信息包含需要更新的sid
                isUpdate = true;
                if (updateDto.getValidity_period() != null) {
                    if (dbLicense.getValidity_period() == null || dbLicense.getExpires_on() < updateDto.getValidity_period().getExpires_on()) {
                        this.updateLicense(dbLicense, updateDto);
                    }
                }
            }
        }
        if (!isUpdate) {
            List<String> sids;
            try {
                String licenseSid = RC4Util.decrypt("Gotapd8", updateDto.getSid());
                sids = JSONUtil.toList(licenseSid, String.class);
            } catch (Throwable e) {
                throw new BizException("Sid.is.wrong");
            }
            for (LicenseDto dbLicense : dbLicenseList) {
                if (StringUtils.isNotEmpty(dbLicense.getSid()) && sids.contains(dbLicense.getSid())) {
                    isUpdate = true;
                    this.updateLicense(dbLicense, updateDto);
                }
            }
        }

        return isUpdate;
    }

    private void updateLicense(LicenseDto dbLicense, LicenseUpdateDto updateDto) {
        dbLicense.setExpires_on(updateDto.getValidity_period().getExpires_on());
        dbLicense.setIssued_on(updateDto.getValidity_period().getIssued_on());
        dbLicense.setLicense(updateDto.getReqDto().getLicense());
        dbLicense.setExpirationDate(new Date(dbLicense.getExpires_on()));
        dbLicense.setLastUpdAt(new Date());
        this.update(repository.getIdQuery(dbLicense.getId()), dbLicense);
    }

    public String getSid() throws Exception {
        if (StringUtils.isEmpty(sid)) {
            sid = SidUtil.generatorSID();
        }
        return sid;
    }

    static class Aes {
        static byte[] iv = new byte[16];

        static String encrypt(String text, String key) throws Throwable {
            SecretKeySpec secretKeySpec = new SecretKeySpec(Base64Decoder.decode(key), "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, new IvParameterSpec(iv));

            byte[] bytes = cipher.doFinal(text.getBytes(StandardCharsets.UTF_8));

            return Base64Encoder.encode(bytes);
        }

        static String decrypt(String text, String key) throws Throwable {
            SecretKeySpec secretKeySpec = new SecretKeySpec(Base64Decoder.decode(key), "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, new IvParameterSpec(iv));

            byte[] bytes = cipher.doFinal(Base64Decoder.decode(text));

            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}