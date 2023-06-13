package io.tapdata.common;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Event;
import com.tapdata.entity.Setting;
import com.tapdata.entity.TapLog;
import com.tapdata.tm.utils.OEMReplaceUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simplejavamail.email.Email;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.email.EmailPopulatingBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WarningEmailEventExecutor extends BaseEmailExecutor implements EventExecutor {

	private Logger logger = LogManager.getLogger(getClass());

	public WarningEmailEventExecutor(SettingService settingService, ConfigurationCenter configCenter) {
		super(settingService, configCenter);
	}

	@Override
	public Event execute(Event event) {

		try {
			if (mailer == null || checkSettings()) {
				synchronized (this) {
					if (mailer == null || checkSettings()) {
						mailer = initMailer();
					}
				}
			}

			if (mailer != null) {
				Setting userSett = settingService.getSetting("smtp.server.user");
				String titlePrefix = settingService.getString("email.title.prefix");
				Setting sendAddress = settingService.getSetting("email.send.address");
				String fromAddress = sendAddress != null && StringUtils.isNotBlank(sendAddress.getValue()) ? sendAddress.getValue() : userSett.getValue();

				Map<String, Object> eventData = event.getEvent_data();
				List<String> customReceivers = event.getReceivers();
				Map<String, Object> oemConfig = OEMReplaceUtil.getOEMConfigMap("email/replace.json");
				String title = (String) eventData.getOrDefault("title", "TAPDATA WARNING");
				if (StringUtils.isNotBlank(titlePrefix)) {
					title = titlePrefix + title;
				}
				title = OEMReplaceUtil.replace(title,oemConfig);
				String messageBody = OEMReplaceUtil.replace(assemblyMessageBody((String) eventData.getOrDefault("message", "")), oemConfig);
				EmailPopulatingBuilder emailPopulatingBuilder = EmailBuilder.startingBlank();
				emailPopulatingBuilder
						.from("", fromAddress)
						.withSubject(title)
						.withHTMLText(messageBody);
				if (CollectionUtils.isNotEmpty(customReceivers)) {
					for (String customReceiver : customReceivers) {
						emailPopulatingBuilder.to("", customReceiver);
					}
				} else {
					String owner = (String) eventData.get("receiver");
					if (StringUtils.isNotEmpty(owner)) {
						emailPopulatingBuilder.to("", owner);
					}
					Setting receiverSetting = settingService.getSetting("email.receivers");
					if (receiverSetting != null) {

						String receivers = receiverSetting.getValue();
						for (String receiver : receivers.split(",")) {
							emailPopulatingBuilder.to("", receiver);
						}

					}
				}
				Email email = emailPopulatingBuilder.buildEmail();

				mailer.sendMail(email);

				event.setConsume_ts(System.currentTimeMillis());
				event.setEvent_status(Event.EVENT_STATUS_SUCCESSED);

			} else {

				Map<String, Object> failedResult = event.getFailed_result();
				if (failedResult == null) {
					failedResult = new HashMap<>();
					event.setFailed_result(failedResult);
				}

				failedResult.put("fail_message", "Please setting SMTP Server config.");
				failedResult.put("ts", System.currentTimeMillis());
				event.setEvent_status(Event.EVENT_STATUS_FAILED);
			}

			logger.info(TapLog.JOB_LOG_0005.getMsg(), event.getName(), event.getEvent_status());
		} catch (Exception e) {
			logger.error(TapLog.ERROR_0001.getMsg(), e.getMessage(), e);
			Map<String, Object> failedResult = event.getFailed_result();
			if (failedResult == null) {
				failedResult = new HashMap<>();
				event.setFailed_result(failedResult);
			}
			String message = e.getMessage();
			failedResult.put("fail_message", message);
			failedResult.put("ts", System.currentTimeMillis());
			event.setEvent_status(Event.EVENT_STATUS_FAILED);
		}

		return event;
	}
}
