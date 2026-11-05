package com.tapdata.tm.externalStorage.vo;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/2 11:08 Create
 * @description
 */
@Data
public class SettingOfSharedCDCEnable {
    Boolean enabled;
    String category;
    String settingKey;
    String externalId;
    String externalName;
    String externalType;

    SettingOfSharedCDCEnable able(boolean able) {
        this.enabled = able;
        return this;
    }

    public SettingOfSharedCDCEnable category(String category) {
        this.category = category;
        return this;
    }

    public SettingOfSharedCDCEnable settingKey(String settingKey) {
        this.settingKey = settingKey;
        return this;
    }

    public SettingOfSharedCDCEnable externalId(String externalId) {
        this.externalId = externalId;
        return this;
    }

    public SettingOfSharedCDCEnable externalName(String externalName) {
        this.externalName = externalName;
        return this;
    }

    public SettingOfSharedCDCEnable externalType(String externalType) {
        this.externalType = externalType;
        return this;
    }

    public static SettingOfSharedCDCEnable unable() {
        return new SettingOfSharedCDCEnable().able(false);
    }

    public static SettingOfSharedCDCEnable enabled() {
        return new SettingOfSharedCDCEnable().able(true);
    }
}
