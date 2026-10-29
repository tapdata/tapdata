package com.tapdata.tm.message.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L2：验证邮件群发在单个收件地址被 SMTP 服务拒绝（550）时的产品行为。
 *
 * <p>被测边界为 {@link MailUtils} 与真实 jakarta.mail 会话之间的交互：使用本机最小 SMTP 服务
 * 模拟「某地址 550、其余地址正常」的真实服务端响应，断言其余收件人仍收到邮件、
 * 且调用方能区分已送达地址与失败地址。不发真实外网邮件，也不依赖数据库。
 */
@Timeout(60)
class MailPartialDeliveryIT {

    private static final String REJECTED_ADDRESS = "bad-recipient@example.com";

    private MinimalSmtpServer smtp;
    private MailUtils mailUtils;
    private GenericApplicationContext context;
    private SettingsService settingsService;

    @BeforeEach
    void setUp() throws Exception {
        smtp = new MinimalSmtpServer();
        smtp.start();

        settingsService = mock(SettingsService.class);
        // MailUtils.addTitlePrefix 通过静态 SpringContextHelper 取 bean；空前缀不改变标题
        when(settingsService.getByCategoryAndKey(any(CategoryEnum.class), any(KeyEnum.class)))
                .thenReturn(setting(""));
        // sendEmail 路径会经 initMailConfig() 重新读取 SMTP 配置，这里指向本机假服务
        when(settingsService.getByCategoryAndKey(eq("SMTP"), eq("smtp.server.host")))
                .thenReturn(setting("127.0.0.1"));
        when(settingsService.getByCategoryAndKey(eq("SMTP"), eq("smtp.server.port")))
                .thenReturn(setting(String.valueOf(smtp.port())));
        when(settingsService.getByCategoryAndKey(eq("SMTP"), eq("smtp.server.user")))
                .thenReturn(setting("sender@example.com"));
        when(settingsService.getByCategoryAndKey(eq("SMTP"), eq("email.send.address")))
                .thenReturn(setting("sender@example.com"));
        when(settingsService.getByCategoryAndKey(eq("SMTP"), eq("smtp.server.password")))
                .thenReturn(setting("secret"));

        context = new GenericApplicationContext();
        context.registerBean(SettingsService.class, () -> settingsService);
        context.registerBean(BlacklistService.class, () -> mock(BlacklistService.class));
        context.refresh();
        SpringContextHelper.applicationContext = context;

        mailUtils = new MailUtils();
        ReflectionTestUtils.setField(mailUtils, "settingsService", settingsService);
    }

    @AfterEach
    void tearDown() {
        SpringContextHelper.applicationContext = null;
        if (context != null) {
            context.close();
        }
        smtp.stop();
    }

    @Test
    @DisplayName("连接测试：一个收件地址被拒时其余收件人仍收到测试邮件，并回报失败地址")
    void should_deliver_test_mail_to_remaining_recipients() {
        MailAccountDto account = account();

        TestResponseDto response = MailUtils.sendHtmlEmail(account,
                new ArrayList<>(Arrays.asList("good1@example.com", REJECTED_ADDRESS, "good2@example.com")),
                "Tapdata Notification:", "This is a test email");

        assertThat(response.isResult())
                .as("部分送达不应被报告为整体失败")
                .isTrue();
        assertThat(response.getValidSentAddresses())
                .containsExactlyInAnyOrder("good1@example.com", "good2@example.com");
        assertThat(response.getFailedAddresses())
                .containsExactly(REJECTED_ADDRESS);
        assertThat(smtp.acceptedRecipients())
                .as("SMTP 服务端应真实收到其余两个地址")
                .containsExactlyInAnyOrder("good1@example.com", "good2@example.com");
    }

    @Test
    @DisplayName("连接测试：所有收件地址都被拒时仍报告失败，并列出失败地址")
    void should_report_failure_when_every_recipient_is_rejected() {
        MailAccountDto account = account();

        TestResponseDto response = MailUtils.sendHtmlEmail(account,
                new ArrayList<>(List.of(REJECTED_ADDRESS)), "Tapdata Notification:", "This is a test email");

        assertThat(response.isResult()).isFalse();
        assertThat(response.getValidSentAddresses()).isEmpty();
        assertThat(response.getFailedAddresses()).containsExactly(REJECTED_ADDRESS);
        assertThat(smtp.acceptedRecipients()).isEmpty();
    }

    private static Settings setting(String value) {
        Settings settings = new Settings();
        settings.setValue(value);
        return settings;
    }

    private MailAccountDto account() {
        return MailAccountDto.builder()
                .host("127.0.0.1")
                .port(smtp.port())
                .user("sender@example.com")
                .pass("secret")
                .from("sender@example.com")
                .protocol("NONE")
                .build();
    }

    /** 最小 SMTP 服务：{@link #REJECTED_ADDRESS} 回 550，其余地址接受并记录。 */
    private static final class MinimalSmtpServer {
        private final java.net.ServerSocket server;
        private final List<String> accepted = new ArrayList<>();
        private volatile boolean running;

        MinimalSmtpServer() throws Exception {
            server = new java.net.ServerSocket(0, 20, java.net.InetAddress.getLoopbackAddress());
        }

        int port() {
            return server.getLocalPort();
        }

        List<String> acceptedRecipients() {
            synchronized (accepted) {
                return new ArrayList<>(accepted);
            }
        }

        void start() {
            running = true;
            Thread thread = new Thread(this::acceptLoop, "minimal-smtp");
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            running = false;
            try {
                server.close();
            } catch (Exception ignored) {
                // 关闭失败不影响断言结果
            }
        }

        private void acceptLoop() {
            while (running && !server.isClosed()) {
                try {
                    java.net.Socket socket = server.accept();
                    Thread handler = new Thread(() -> handle(socket), "minimal-smtp-session");
                    handler.setDaemon(true);
                    handler.start();
                } catch (Exception e) {
                    return;
                }
            }
        }

        private void handle(java.net.Socket socket) {
            try (java.net.Socket s = socket;
                 java.io.BufferedReader in = new java.io.BufferedReader(
                         new java.io.InputStreamReader(s.getInputStream()));
                 java.io.BufferedWriter out = new java.io.BufferedWriter(
                         new java.io.OutputStreamWriter(s.getOutputStream()))) {
                write(out, "220 minimal ESMTP");
                boolean inData = false;
                int authStep = 0;
                String line;
                while ((line = in.readLine()) != null) {
                    if (inData) {
                        if (".".equals(line)) {
                            inData = false;
                            write(out, "250 OK queued");
                        }
                        continue;
                    }
                    String upper = line.toUpperCase(java.util.Locale.ROOT);
                    if (authStep == 1) {
                        authStep = 2;
                        write(out, "334 UGFzc3dvcmQ6");
                    } else if (authStep == 2) {
                        authStep = 0;
                        write(out, "235 2.7.0 Authentication successful");
                    } else if (upper.startsWith("EHLO") || upper.startsWith("HELO")) {
                        write(out, "250-minimal");
                        write(out, "250-AUTH LOGIN PLAIN");
                        write(out, "250 8BITMIME");
                    } else if (upper.startsWith("AUTH LOGIN")) {
                        authStep = 1;
                        write(out, "334 VXNlcm5hbWU6");
                    } else if (upper.startsWith("AUTH")) {
                        write(out, "235 2.7.0 Authentication successful");
                    } else if (upper.startsWith("MAIL FROM")) {
                        synchronized (accepted) {
                            accepted.clear();
                        }
                        write(out, "250 OK");
                    } else if (upper.startsWith("RCPT TO")) {
                        String address = line.substring(line.indexOf(':') + 1).trim().replaceAll("[<>]", "");
                        if (REJECTED_ADDRESS.equals(address)) {
                            write(out, "550 5.1.0 Address rejected.");
                        } else {
                            synchronized (accepted) {
                                accepted.add(address);
                            }
                            write(out, "250 OK");
                        }
                    } else if (upper.equals("DATA")) {
                        write(out, "354 End data");
                        inData = true;
                    } else if (upper.equals("RSET")) {
                        write(out, "250 OK");
                    } else if (upper.equals("QUIT")) {
                        write(out, "221 Bye");
                        return;
                    } else {
                        write(out, "500 Unrecognized");
                    }
                }
            } catch (Exception ignored) {
                // 会话异常由被测代码的失败分支覆盖，不在此处断言
            }
        }

        private void write(java.io.BufferedWriter out, String line) throws java.io.IOException {
            out.write(line + "\r\n");
            out.flush();
        }
    }
}
