package com.tapdata.config;

import com.tapdata.utils.FileUtils;
import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
@Data
public class Config {
    private String path;
    private String packageName;
    private String basePackageName;
    private String baseClassName;
    //每次构造的时候，最外层的实体才是我们的model对象，内嵌的这个都会为false
    private boolean outer = true;

    private String model = FileUtils.readResourceFile("model");
    private String serviceModel = FileUtils.readResourceFile("modelService");
    private String repositoryModel = FileUtils.readResourceFile("modelRepository");
    private String controllerModel = FileUtils.readResourceFile("modelController");
    public boolean checkOuter() {
        if (outer) {
            outer = false;
            return true;
        }
        return false;
    }
}
