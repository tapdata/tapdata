package com.tapdata.tm.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/7/29 11:26 Create
 * @description
 */

@Service
@Data
public class ApplicationConfig {
    @Value("${application.title:TM}")
    String title;

    @Value("${application.version}")
    String version;

    @Value("${application.commit_version}")
    String commitVersion;

    @Value("${application.description:Tapdata Manager}")
    String description;

    @Value("${application.build}")
    String build;

    @Value("${application.admin-account:admin@admin.com}")
    String adminAccount;
}
