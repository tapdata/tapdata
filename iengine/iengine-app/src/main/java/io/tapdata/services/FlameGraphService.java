package io.tapdata.services;

import io.tapdata.service.skeleton.annotation.RemoteService;

import java.io.File;
import java.lang.management.ManagementFactory;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

@RemoteService
public class FlameGraphService {

	private final Logger log = LogManager.getLogger(FlameGraphService.class);

	public static final String TAG = FlameGraphService.class.getSimpleName();

	private String asyncProfilerPath = "./async-profiler";

	FlameGraphService() {
		// 寻找 async-profiler 路径, 可能在当前路径, 也可能在父路径
		if (!new File(asyncProfilerPath).exists()) {
			asyncProfilerPath = "../async-profiler";
		}
	}

	@Data
	private static class Request {
	}

	@Data
	@NoArgsConstructor
	private static class Response {
		private byte[] content;
		public Response(byte[] content) {
			this.content = content;
		}
	}

	public Response memory() throws Throwable {
		// 删除 memory.html 文件
		FileUtils.deleteQuietly(new File("memory.html"));

		String name = ManagementFactory.getRuntimeMXBean().getName();
		// 提取 PID
		String pid = name.split("@")[0];
		// 1. 执行命令:
		String command = asyncProfilerPath+"/bin/asprof -e alloc -d " + 15 + " -f ./memory.html " + pid;

		if (!new File(asyncProfilerPath).exists()) {
			return new Response("async-profiler not found".getBytes());
		}

		byte[] content = null;
		try {
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor( 20, TimeUnit.SECONDS);
			content = FileUtils.readFileToByteArray(new File("memory.html"));
		} catch (Exception e) {
			return new Response(e.getMessage().getBytes());
		}
		return new Response(content);
	}

	public Response cpu() throws Throwable {
		// 删除 cpu.html 文件
		FileUtils.deleteQuietly(new File("cpu.html"));

		String name = ManagementFactory.getRuntimeMXBean().getName();
		// 提取 PID
		String pid = name.split("@")[0];
		// 1. 执行命令:
		String command = asyncProfilerPath+"/bin/asprof -e cpu -d " + 15 + " -f ./cpu.html " + pid;
		if (!new File(asyncProfilerPath).exists()) {
			return new Response("async-profiler not found".getBytes());
		}

		byte[] content = null;
		try {
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor(20, TimeUnit.SECONDS);
			content = FileUtils.readFileToByteArray(new File("cpu.html"));
		} catch (Exception e) {
			return new Response(e.getMessage().getBytes());
		}
		return new Response(content);
	}

	public Response jstack() throws Throwable {
		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		boolean lockedMonitors = false;
		boolean lockedSynchronizers = false;
		ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(lockedMonitors, lockedSynchronizers);
		String threadDump = "";
		for (ThreadInfo threadInfo : threadInfos) {
			threadDump += threadInfo.toString();
			threadDump += "\n";
		}
		return new Response(threadDump.getBytes());
	}
}
