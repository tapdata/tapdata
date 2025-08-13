package com.tapdata.tm.modules.dto;

import lombok.Data;
import lombok.Getter;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/1 11:17 Create
 * @description
 */
@Data
public class PathSetting {
    public static final List<PathSetting> DEFAULT_PATH_SETTING = List.of(
            PathSetting.of(PathSettingType.DEFAULT_POST),
            PathSetting.of(PathSettingType.DEFAULT_GET)
    );

    /**
     * @see PathSettingType
     * */
    PathSettingType type;

    String path;

    String method;



    public static PathSetting of(PathSettingType type) {
        PathSetting pathSetting = new PathSetting();
        pathSetting.setType(type);
        pathSetting.setPath(type.getDefaultPath());
        pathSetting.setMethod(type.getMethod());
        return pathSetting;
    }

    @Getter
    public enum PathSettingType {
        DEFAULT_POST("DEFAULT_POST", "/find", "POST"),
        DEFAULT_GET("DEFAULT_GET", "", "GET")
        ;

        final String code;
        final String defaultPath;
        final String method;
        PathSettingType(String code, String defaultPath, String method) {
            this.code = code;
            this.defaultPath = defaultPath;
            this.method = method;
        }
    }
}
