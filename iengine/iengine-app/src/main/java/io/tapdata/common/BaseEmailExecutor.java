package io.tapdata.common;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Setting;
import com.tapdata.entity.TapLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.MailerBuilder;

/**
 * @author samuel
 * @Description
 * @create 2020-08-12 16:15
 **/
public class BaseEmailExecutor {

	private static Logger logger = LogManager.getLogger(BaseEmailExecutor.class);

	protected long lastLoadSettingTS;

	protected SettingService settingService;

	protected ConfigurationCenter configCenter;

	protected Mailer mailer;

	public BaseEmailExecutor(SettingService settingService, ConfigurationCenter configCenter) {
		this.settingService = settingService;
		this.configCenter = configCenter;

		mailer = initMailer();
	}

	protected Mailer initMailer() {
		Mailer mailer = null;
		try {
			Setting hostSett = settingService.getSetting("smtp.server.host");
			Setting portSett = settingService.getSetting("smtp.server.port");
			Setting userSett = settingService.getSetting("smtp.server.user");
			Setting passwordSett = settingService.getSetting("smtp.server.password");
			Setting encryptTypeSett = settingService.getSetting("email.server.tls");
			Setting sendAddress = settingService.getSetting("email.send.address");

			if (hostSett != null && portSett != null && userSett != null &&
					StringUtils.isNotBlank(hostSett.getValue()) && StringUtils.isNotBlank(portSett.getValue()) && StringUtils.isNotBlank(userSett.getValue())) {

				lastLoadSettingTS = getLastSettingTS(hostSett, portSett, userSett, passwordSett, encryptTypeSett, sendAddress);
				String host = hostSett.getValue();
				String port = portSett.getValue();
				String user = userSett.getValue();
				String password = StringUtils.isNotBlank(passwordSett.getValue()) ? passwordSett.getValue() : null;

				mailer = MailerBuilder
						.withSMTPServer(host, Integer.valueOf(port), user, password).withDebugLogging(true)
						.withProperty("tls.rejectUnauthorized", "false")
						.withProperty("mail.smtp.ssl.enable", StringUtils.equalsAnyIgnoreCase(encryptTypeSett.getValue(), "SSL") ? "true" : "false")
						.withProperty("mail.smtp.starttls.enable", StringUtils.equalsAnyIgnoreCase(encryptTypeSett.getValue(), "TLS") ? "true" : "false")
						.buildMailer();
			}
		} catch (Exception e) {
			logger.error(TapLog.ERROR_0002.getMsg(), e.getMessage(), e);
		}

		return mailer;
	}

	protected boolean checkSettings() {
		Setting hostSett = settingService.getSetting("smtp.server.host");
		Setting portSett = settingService.getSetting("smtp.server.port");
		Setting userSett = settingService.getSetting("smtp.server.user");
		Setting passwordSett = settingService.getSetting("smtp.server.password");
		Setting encryptTypeSett = settingService.getSetting("email.server.tls");
		Setting sendAddress = settingService.getSetting("email.send.address");

		return lastLoadSettingTS < hostSett.getLast_update() ||
				lastLoadSettingTS < portSett.getLast_update() ||
				lastLoadSettingTS < userSett.getLast_update() ||
				lastLoadSettingTS < passwordSett.getLast_update() ||
				lastLoadSettingTS < (encryptTypeSett != null ? encryptTypeSett.getLast_update() : 0L) ||
				lastLoadSettingTS < (sendAddress != null ? sendAddress.getLast_update() : 0);
	}

	protected long getLastSettingTS(Setting hostSett, Setting portSett, Setting userSett, Setting passwordSett, Setting encryptTypeSett, Setting sendAddress) {
		long hostSettLastUpdate = hostSett.getLast_update();
		long portSettLastUpdate = portSett.getLast_update();
		long userSettLastUpdate = userSett.getLast_update();
		long passwordSettLast_update = passwordSett.getLast_update();
		long encryptTypeSettLast_update = encryptTypeSett != null ? encryptTypeSett.getLast_update() : 0;
		long sendAddressSettLast_update = sendAddress != null ? sendAddress.getLast_update() : 0;

		return NumberUtils.max(hostSettLastUpdate, portSettLastUpdate, userSettLastUpdate, passwordSettLast_update, encryptTypeSettLast_update, sendAddressSettLast_update);
	}

	protected String assemblyMessageBody(String message) {
		String template = "<!DOCTYPE html>\n" +
				"<html>\n" +
				"<head>\n" +
				"<meta charset=\"utf-8\" />\n" +
				"</head>\n" +
				"<body>\n" +
				"Hello there,<br />\n" +
				"<br />\n" +
				message +
				"</p>\n" +
				"<br />" +
				"<br />" +
				"This mail was sent by Tapdata. " +
				"</body>\n" +
				"</html>";

		return template;
	}
}
