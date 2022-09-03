package com.tapdata.tm;

import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
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

	private static final String TAG = TMApplication.class.getSimpleName();

	public static void main(String[] args) {
		CommonUtils.setProperty("tap_verbose", "true");

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

		TapLogger.setLogListener(new TapLogger.LogListener() {
			@Override
			public void debug(String msg) {
				log.info(msg);
//				System.out.println(msg);
			}

			@Override
			public void info(String msg) {
				log.info(msg);
//					System.out.println(log);
			}

			@Override
			public void warn(String msg) {
				log.warn(msg);
//				System.out.println(msg);
			}

			@Override
			public void error(String msg) {
				log.error(msg);
			}

			@Override
			public void fatal(String msg) {
				log.error(msg);
			}

			@Override
			public void memory(String msg) {
				log.info(msg);
			}
		});

		TapLogger.debug(TAG, "TapRuntime will start");
		//TODO should use TM way to get the mongo uri.
		CommonUtils.setProperty("TAPDATA_MONGO_URI", "mongodb://127.0.0.1:27017/tapdata?authSource=admin");
		TapRuntime.getInstance();
		TapLogger.debug(TAG, "TapRuntime initialized");
	}
}
