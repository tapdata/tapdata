package com.tapdata.tm;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.TimeZone;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 6:54 上午
 * @description
 */
@Import(cn.hutool.extra.spring.SpringUtil.class)
@ServletComponentScan("com.tapdata.tm.monitor.servlet")
@SpringBootApplication
@EnableMongoAuditing
@EnableMongoRepositories
@EnableAsync
@Slf4j
public class TMApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TMApplication.class, args);
		SpringContextHelper.applicationContext = applicationContext;

		UserService userService = applicationContext.getBean(UserService.class);
		Query query = new Query(Criteria.where("email").is("admin@admin.com"));
		query.fields().include("accessCode");

		UserDto userDto = userService.findOne(query);
		if (userDto != null) {
			log.info("admin access code is {}", userDto.getAccessCode());
		}

		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));

		DefaultDataDirectoryService bean = applicationContext.getBean(DefaultDataDirectoryService.class);
		UserDetail userDetail = userService.loadUserByUsername("admin@admin.com");

		bean.addPdkIds(userDetail);
		bean.addConnections(userDetail);

	}
}
