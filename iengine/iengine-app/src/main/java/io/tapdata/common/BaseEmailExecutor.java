package io.tapdata.common;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Event;
import com.tapdata.entity.Setting;
import com.tapdata.entity.TapLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.MailerBuilder;

import javax.mail.Address;
import javax.mail.SendFailedException;

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
			Setting proxyHostSett = settingService.getSetting("smtp.proxy.host");
			Setting proxyPortSett = settingService.getSetting("smtp.proxy.port");

			if (hostSett != null && portSett != null && userSett != null &&
					StringUtils.isNotBlank(hostSett.getValue()) && StringUtils.isNotBlank(portSett.getValue()) && StringUtils.isNotBlank(userSett.getValue())) {

				lastLoadSettingTS = getLastSettingTS(hostSett, portSett, userSett, passwordSett, encryptTypeSett, sendAddress);
				String host = hostSett.getValue();
				String port = portSett.getValue();
				String user = userSett.getValue();
				String password = StringUtils.isNotBlank(passwordSett.getValue()) ? passwordSett.getValue() : null;
				String proxyHost = null;
				String proxyPort = null;
				if (null != proxyHostSett && null != proxyPortSett){
					proxyHost = proxyHostSett.getValue();
					proxyPort = proxyPortSett.getValue();
				}
				MailerBuilder.MailerRegularBuilder mailerBuilder = MailerBuilder
						.withSMTPServer(host, Integer.valueOf(port), user, password).withDebugLogging(true)
						.withProperty("tls.rejectUnauthorized", "false")
						.withProperty("mail.smtp.ssl.enable", StringUtils.equalsAnyIgnoreCase(encryptTypeSett.getValue(), "SSL") ? "true" : "false")
						.withProperty("mail.smtp.starttls.enable", StringUtils.equalsAnyIgnoreCase(encryptTypeSett.getValue(), "TLS") ? "true" : "false")
						// 群发时单个收件地址被拒（如 550）不应导致整封邮件失败，其余收件人继续投递
						.withProperty("mail.smtp.sendpartial", "true");
				if (StringUtils.isNotBlank(proxyHost) && StringUtils.isNotBlank(proxyPort)) {
					mailerBuilder.withProxy(proxyHost, Integer.valueOf(proxyPort), user, password);
				}
				mailer = mailerBuilder.buildMailer();
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

	/**
	 * 群发时若仅部分收件地址被拒绝（如 550），邮件仍已投递给其余收件人。
	 * 这类异常不应被当作整体失败，否则会触发失败重试导致重复通知。
	 *
	 * @param event  当前邮件事件；部分送达时会被标记为成功
	 * @param logger 调用方日志器，保持各 executor 自己的日志来源
	 * @return 部分送达返回 true，调用方应直接返回该事件；否则返回 false 交调用方按失败处理
	 */
	protected boolean completeAsPartialSent(Event event, Exception e, Logger logger) {
		SendFailedException failure = findSendFailedException(e);
		if (failure == null || !hasDeliveredRecipient(failure)) {
			return false;
		}
		logger.warn("{} partial sent: {}", TapLog.ERROR_0001.getMsg(), partialSentMessage(failure));
		event.setConsume_ts(System.currentTimeMillis());
		event.setEvent_status(Event.EVENT_STATUS_SUCCESSED);
		logger.info(TapLog.JOB_LOG_0005.getMsg(), event.getName(), event.getEvent_status());
		return true;
	}

	private static SendFailedException findSendFailedException(Throwable error) {
		Throwable current = error;
		while (current != null) {
			if (current instanceof SendFailedException) {
				return (SendFailedException) current;
			}
			current = current.getCause();
		}
		return null;
	}

	/** 是否至少有一个收件地址已实际投递；仅全部被拒时才应按失败处理。 */
	private static boolean hasDeliveredRecipient(SendFailedException failure) {
		Address[] validSent = failure.getValidSentAddresses();
		return validSent != null && validSent.length > 0;
	}

	protected String partialSentMessage(SendFailedException failure) {
		return "Partial delivery, failed addresses: " + describe(failure.getInvalidAddresses())
				+ describe(failure.getValidUnsentAddresses());
	}

	private String describe(Address[] addresses) {
		if (addresses == null || addresses.length == 0) {
			return "";
		}
		StringBuilder builder = new StringBuilder();
		for (Address address : addresses) {
			if (builder.length() > 0) {
				builder.append(", ");
			}
			builder.append(address);
		}
		return "[" + builder + "]";
	}
}
