package egine;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author samuel
 * @Description
 * @create 2022-11-08 14:54
 **/
public class TestJetStop {
	public static void main(String[] args) {
		Config config = new Config();
		config.getJetConfig().setEnabled(true);
		JoinConfig joinConfig = new JoinConfig();
		joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
		NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setJoin(joinConfig);
		config.setNetworkConfig(networkConfig);
		config.setInstanceName(TestJetStop.class.getSimpleName());
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		DAG dag = new DAG();
		AtomicInteger vertexId = new AtomicInteger();
		Vertex vertex1 = new Vertex(vertexId.incrementAndGet() + "", Processor1::new);
		vertex1.localParallelism(1);
		Vertex vertex2 = new Vertex(vertexId.incrementAndGet() + "", Processor2::new);
		vertex2.localParallelism(1);
		Vertex vertex3 = new Vertex(vertexId.incrementAndGet() + "", Processor3::new);
		vertex3.localParallelism(1);
		dag.vertex(vertex1);
		dag.vertex(vertex2);
		dag.vertex(vertex3);
		dag.edge(Edge.from(vertex1).to(vertex2).setConfig(new EdgeConfig().setQueueSize(100)));
		dag.edge(Edge.from(vertex2).to(vertex3).setConfig(new EdgeConfig().setQueueSize(100)));
		JobConfig jobConfig = new JobConfig();
		jobConfig.setName("Test");
		jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
		Job job = hazelcastInstance.getJet().newJob(dag, jobConfig);
		try {
			Thread.sleep(5 * 1000L);
		} catch (InterruptedException ignored) {
		}
		job.suspend();
		System.out.printf("%s suspend job%n", Instant.now());
		while (true) {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException ignored) {
			}
			if (job.getStatus().equals(JobStatus.SUSPENDED)) {
				System.out.printf("%s cancel job%n", Instant.now());
				job.cancel();
				break;
			}
		}
		hazelcastInstance.shutdown();
		System.out.println("Test complete");
	}

	static abstract class AbstractTestP extends AbstractProcessor {
		protected String vertexName;

		@Override
		protected void init(@NotNull Context context) throws Exception {
			super.init(context);
			vertexName = context.vertexName();
		}

		@Override
		public void close() throws Exception {
			System.out.println(vertexName + " close");
			super.close();
		}
	}

	static class Processor1 extends AbstractTestP {
		AtomicLong id = new AtomicLong(0L);
		long num = 1000000;

		@Override
		public boolean complete() {
			System.out.println(vertexName + " complete");
			for (int i = 0; i < 1000; i++) {
				Map<String, Object> record = new HashMap<>();
				record.put("id", id.incrementAndGet());
				record.put("f1", RandomStringUtils.randomAlphabetic(10));
				record.put("f2", RandomStringUtils.randomAlphabetic(10));
				record.put("f3", RandomStringUtils.randomAlphabetic(10));
				record.put("f4", RandomStringUtils.randomAlphabetic(10));
				emit(record);
				num--;
			}
			return num == 0;
		}

		private void emit(Object item) {
			while (tryEmit(item)) {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException ignored) {
				}
				break;
			}
		}
	}

	static class Processor2 extends AbstractTestP {
		@Override
		protected boolean tryProcess(int ordinal, @NotNull Object item) throws Exception {
			Thread.sleep(1000L);
			return tryEmit(item);
		}
	}

	static class Processor3 extends AbstractTestP {
		private Object item = null;
		private AtomicLong counter = new AtomicLong();

		@Override
		public void process(int ordinal, @NotNull Inbox inbox) {
			Object poll = inbox.poll();
			if (null == poll) {
				return;
			}
			item = poll;
			System.out.printf(vertexName + " process %s%n", counter.incrementAndGet());
		}

		@Override
		public boolean saveToSnapshot() {
			System.out.println(vertexName + " save to snapshot: " + item);
			return true;
		}
	}
}
