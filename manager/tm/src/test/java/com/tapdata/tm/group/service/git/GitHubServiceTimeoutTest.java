package com.tapdata.tm.group.service.git;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kohsuke.github.GitHub;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Git 网络调用的超时接线。
 *
 * <p>这些调用原先全是裸的：JGit 默认 {@code timeout=0}（无限），hub4j 默认 connector 没有读超时。
 * 网络卡住 = @Async 线程永久挂住 = 导出记录停在 exporting = 分组被永久锁死。</p>
 *
 * <p>这里测的是<b>接线本身能不能在运行期成立</b>——编译通过不代表
 * {@code withConnector(ImpatientHttpConnector)} 这条路在 hub4j 1.330 上还活着，
 * 也不代表配大了的分钟数不会把 {@code setReadTimeout} 撑成负数。</p>
 */
class GitHubServiceTimeoutTest {

	private GitHubService gitHubService;

	@BeforeEach
	void setUp() {
		gitHubService = new GitHubService();
	}

	private GitHub createClient() {
		return (GitHub) ReflectionTestUtils.invokeMethod(
				gitHubService, "createGitHubClient", "https://github.com/acme/repo.git", "token123");
	}

	@Test
	@DisplayName("默认 60 分钟：JGit 超时是 3600 秒，不是 0")
	void defaultTimeoutIsNotInfinite() {
		assertEquals(3600, gitHubService.gitTimeoutSeconds());
		assertEquals(60, gitHubService.exportTimeoutMinutes());
	}

	@Test
	@DisplayName("配置为 0 或负数时回落到 1 分钟，绝不退化成无限等待")
	void nonPositiveConfigFallsBackToOneMinute() {
		ReflectionTestUtils.setField(gitHubService, "exportTimeoutMinutes", 0, int.class);
		assertEquals(60, gitHubService.gitTimeoutSeconds());

		ReflectionTestUtils.setField(gitHubService, "exportTimeoutMinutes", -5, int.class);
		assertEquals(60, gitHubService.gitTimeoutSeconds());
	}

	@Test
	@DisplayName("带超时的 connector 能真的建出 GitHub 客户端")
	void clientBuildsWithTimeoutConnector() {
		assertNotNull(assertDoesNotThrow(this::createClient));
	}

	@Test
	@DisplayName("分钟数配到会撑爆 int 毫秒的量级也不抛（夹回 Integer.MAX_VALUE）")
	void hugeTimeoutDoesNotOverflow() {
		// 40000 * 60_000 = 2.4e9 > Integer.MAX_VALUE，直接用 int 相乘会翻成负数
		ReflectionTestUtils.setField(gitHubService, "exportTimeoutMinutes", 40000, int.class);
		assertNotNull(assertDoesNotThrow(this::createClient));
	}
}
