package io.tapdata.proxy.connection;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.node.connection.NodeConnection;
import io.tapdata.modules.api.net.service.node.connection.entity.NodeMessage;
import io.tapdata.modules.api.proxy.constants.ProxyConstants;
import io.tapdata.entity.tracker.MessageTracker;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;
import io.tapdata.pdk.core.utils.state.StateMachine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class NodeConnectionHttpImpl implements NodeConnection {
	public static final String STATE_NONE = "None";
	public static final String STATE_FIND_BEST_IP = "Find Best IP";
	public static final String STATE_READY = "Ready";
	public static final String STATE_TERMINATED = "Terminated";
	private static final String TAG = NodeConnectionHttpImpl.class.getSimpleName();
	private StateMachine<String, NodeConnectionHttpImpl> stateMachine;
	private NodeRegistry nodeRegistry;
	private BiConsumer<NodeRegistry, String> nodeRegistryReasonSelfDestroy;
	private String terminateReason;

	private final int MAX_RETRY = 3;
	private final int RETRY_PERIOD_SECONDS = 15;
	private int retryTimes = MAX_RETRY;
	private long touch;

	private final List<String> workableIps = new CopyOnWriteArrayList<>();
	private SingleThreadBlockingQueue<NodeMessage> asyncQueue;
	@Override
	public void init(NodeRegistry nodeRegistry, BiConsumer<NodeRegistry, String> nodeRegistryReasonSelfDestroy) {
		if(stateMachine == null) {
			this.nodeRegistry = nodeRegistry;
			this.nodeRegistryReasonSelfDestroy = nodeRegistryReasonSelfDestroy;
			stateMachine = new StateMachine<>(NodeRegistry.class.getSimpleName() + "#" + nodeRegistry.id(), STATE_NONE, this);
//			StateOperateRetryHandler<String, NodeConnectionHttpImpl> findBestIPHandler = StateOperateRetryHandler.build(stateMachine, ExecutorsManager.getInstance().getScheduledExecutorService()).setMaxRetry(5).setRetryInterval(2000L)
//                .setOperateListener(this::handleFindBestIP)
//                .setOperateFailedListener(this::handleFindBestIPFailed);
			stateMachine.configState(STATE_NONE, stateMachine.execute(this::handleNone).nextStates(STATE_FIND_BEST_IP, STATE_TERMINATED))
					.configState(STATE_FIND_BEST_IP, stateMachine.execute(this::handleFindBestIP).nextStates(STATE_NONE, STATE_READY, STATE_TERMINATED))
					.configState(STATE_READY, stateMachine.execute(this::handleReady).nextStates(STATE_NONE, STATE_TERMINATED))
					.configState(STATE_TERMINATED, stateMachine.execute(this::handleTerminated).nextStates())
					.errorOccurred(this::handleError);
			stateMachine.gotoState(STATE_FIND_BEST_IP, "Started");
//			stateMachine.enableAsync(Executors.newSingleThreadExecutor()); //Use one thread for a worker
//			stateMachine.gotoState(STATE_FIND_BEST_IP, "Start finding best ip from node registry " + nodeRegistry);
			asyncQueue = new SingleThreadBlockingQueue<NodeMessage>("NodeConnection " + nodeRegistry.id())
					.withHandleSize(10)
					.withMaxSize(200)
					.withMaxWaitMilliSeconds(0)
					.withExecutorService(ExecutorsManager.getInstance().getExecutorService())
					.withHandler(this::handleBatch)
					.withErrorHandler(this::handleBatchError)
					.start();
			touch();
		} else {
			throw new CoreException(NetErrors.ILLEGAL_STATE, "Node connection has been initialized already");
		}
	}

	private void handleBatchError(List<NodeMessage> nodeMessages, Throwable throwable) {
		TapLogger.debug(TAG, "Send {} messages to {} failed, {}", nodeMessages.size(), nodeRegistry.id(), throwable.getMessage());
		for(NodeMessage nodeMessage : nodeMessages) {
			nodeMessage.accept(null, throwable);
		}
	}

	private void handleBatch(List<NodeMessage> nodeMessages) throws IOException {
		if(nodeMessages != null) {
			if(workableIps.isEmpty()) {
				TapLogger.debug(TAG, "workableIps is empty, {} messages was ignored", nodeMessages.size());
				Throwable throwable = new CoreException(NetErrors.NO_WORKABLE_IP, "No workable ip for node {} to send {} messages", nodeRegistry.id(), nodeMessages.size());
				for(NodeMessage nodeMessage : nodeMessages) {
					nodeMessage.accept(null, throwable);
				}
				return;
			}
			Integer port = nodeRegistry.getHttpPort();
			String ip = workableIps.get(0);
			String url = "http://" + ip + ":" + port + "/api/proxy/internal?key=" + ProxyConstants.INTERNAL_KEY;
			for(NodeMessage nodeMessage : nodeMessages) {
				try {
					NodeMessage responseMessage = post(url, nodeMessage);
					if(responseMessage != null && responseMessage.getData() != null && nodeMessage.getResponseClass() != null)
						nodeMessage.accept(fromJson(new String(responseMessage.getData(), StandardCharsets.UTF_8), nodeMessage.getResponseClass()), null);
					else
						nodeMessage.accept(null, new CoreException(NetErrors.ILLEGAL_PARAMETERS, "ResponseMessage is not legal, {}", responseMessage));
				} catch(Throwable throwable1) {
					nodeMessage.accept(null, throwable1);
				}
			}
		}
	}

	private void touch() {
		touch = System.currentTimeMillis();
	}

	private void handleNone(NodeConnectionHttpImpl nodeConnectionHttp, StateMachine<String, NodeConnectionHttpImpl> stateMachine) {
		if(!workableIps.isEmpty())
			workableIps.clear();

		stateMachine.gotoState(STATE_FIND_BEST_IP, FormatUtils.format("Start finding best ip from none state from nodeRegistry {}", nodeRegistry));
	}

//	private void handleFindBestIPFailed(boolean willRetry, int retryCount, int maxRetry, NodeConnectionHttpImpl nodeConnectionHttp, StateMachine<String, NodeConnectionHttpImpl> stateMachine) {
//		if(willRetry) {
//			stateMachine.gotoState(STATE_FIND_BEST_IP, FormatUtils.format("Retry find best ip, retryCount {}, retryMax {} for nodeRegistry {}", retryCount, maxRetry, nodeRegistry));
//		} else {
//			stateMachine.gotoState(STATE_TERMINATED, FormatUtils.format("Terminated because no ip available, node registry {}", nodeRegistry));
//		}
//	}

	private void handleError(Throwable throwable, String fromState, String toState, NodeConnectionHttpImpl nodeConnectionHttp, StateMachine<String, NodeConnectionHttpImpl> stateMachine) {

	}

	private void handleTerminated(NodeConnectionHttpImpl nodeConnectionHttp, StateMachine<String, NodeConnectionHttpImpl> stateMachine) {
		if(nodeRegistryReasonSelfDestroy != null)
			nodeRegistryReasonSelfDestroy.accept(nodeRegistry, terminateReason);
	}

	private void handleReady(NodeConnectionHttpImpl nodeConnectionHttp, StateMachine<String, NodeConnectionHttpImpl> stateMachine) {
		touch();
	}

	private void handleFindBestIP(NodeConnectionHttpImpl nodeConnectionHttp, StateMachine<String, NodeConnectionHttpImpl> stateMachine) {
		List<String> ips = nodeRegistry.getIps();
		Integer port = nodeRegistry.getHttpPort();
		if(ips == null || ips.isEmpty() || port == null) {
			terminateReason = FormatUtils.format("No ips {} or port {} while find best ip", ips, port);
			stateMachine.gotoState(STATE_TERMINATED, terminateReason);
			return;
		}

		ThreadPoolExecutor theExecutorService = new ThreadPoolExecutor(3, 3, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1000), new ThreadFactoryBuilder().setNameFormat(FormatUtils.format("NodeConnection {}", nodeRegistry.id())).build(), (r, executor) -> {
			TapLogger.error(TAG, "Thread is rejected, runnable {} pool {}", r, executor.toString());
		});
		theExecutorService.allowCoreThreadTimeOut(true);
		List<String> pendingIps = new ArrayList<>(ips);
		for(String ip : ips) {
//			if(ip.equals("127.0.0.1") || ip.equals("localhost"))
//				continue;
			long time = System.currentTimeMillis();
			theExecutorService.execute(() -> {
				String url = "http://" + ip + ":" + port + "/api/proxy/internal?key=" + ProxyConstants.INTERNAL_KEY + "&ping=" + nodeRegistry.id();
				try {
					post(url, null);
					workableIps.add(ip);

					if(stateMachine.getCurrentState().equals(STATE_FIND_BEST_IP)) {
						synchronized (this) {
							if(stateMachine.getCurrentState().equals(STATE_FIND_BEST_IP)) {
								TapLogger.debug(TAG, "Found best ip {}, enter ready state, nodeRegistry {}", ip, nodeRegistry);
								stateMachine.gotoState(STATE_READY, FormatUtils.format("Found best ip {}, connection is ready", ip));
							}
						}
					}
					TapLogger.debug(TAG, "ip {} is workable, takes {}", ip, System.currentTimeMillis() - time);
				} catch (IOException e) {
					TapLogger.debug(TAG, "Ping url {} failed(IOException), {}, {}", url, e.getClass().getSimpleName(), e.getMessage());
				} catch (Throwable throwable) {
					TapLogger.debug(TAG, "Ping url {} failed(Throwable), {}, {}", url, throwable.getClass().getSimpleName(), throwable.getMessage());
				} finally {
					pendingIps.remove(ip);
					if(pendingIps.isEmpty()) {
						if(stateMachine.getCurrentState().equals(STATE_FIND_BEST_IP) && workableIps.isEmpty()) {
							if(retryTimes-- > 0) {
								TapLogger.debug(TAG, "Will retry after {} seconds", RETRY_PERIOD_SECONDS);
								synchronized (this) {
									try {
										this.wait(TimeUnit.SECONDS.toMillis(RETRY_PERIOD_SECONDS));
									} catch (InterruptedException e) {
										TapLogger.debug(TAG, "Wait {} seconds to retry is interrupted, {}", RETRY_PERIOD_SECONDS, e.getMessage());
									}
								}
								theExecutorService.shutdownNow();
								if(stateMachine.getCurrentState().equals(STATE_FIND_BEST_IP))
									stateMachine.gotoState(STATE_NONE, FormatUtils.format("Enter none state to find best ip again, ips {}, retryTimes {} nodeRegistry {}", ips, retryTimes, nodeRegistry));
							} else {
								terminateReason = FormatUtils.format("Can not find best ip after tried {} times, nodeRegistry {}", MAX_RETRY, nodeRegistry);
								stateMachine.gotoState(STATE_TERMINATED, terminateReason);
							}
						}
					}
				}
			});
		}
	}

	private NodeMessage post(String url, NodeMessage nodeMessage) throws IOException {
		touch();
		TapLogger.debug(TAG, "post url {} nodeMessage {}", url, nodeMessage);
		URL theUrl = new URL(url);
		HttpURLConnection connection = (HttpURLConnection) theUrl.openConnection();
		connection.setRequestMethod("POST");
		connection.setConnectTimeout(10000);
		connection.setReadTimeout(65000);
		connection.setDoInput(true);
		try {
			if(nodeMessage != null) {
				connection.setDoOutput(true);
//				connection.setRequestProperty( "Content-Type", "application/json");
				try(OutputStream outputStream = connection.getOutputStream()) {
					nodeMessage.to(outputStream);
				}
			}
			connection.connect();
			int code = connection.getResponseCode();
			if(code >= 200 && code < 300) {
				if(code == 200) {
					try(InputStream inputStream = connection.getInputStream()) {
						NodeMessage responseMessage = new NodeMessage();
						responseMessage.from(inputStream);
						TapLogger.debug(TAG, "post url {} nodeMessage {} received {}", url, nodeMessage, responseMessage);
						return responseMessage;
//					String json = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
//					if(StringUtils.isNotBlank(json)) {
//						JSONObject result = JSON.parseObject(json);
//						if(result != null && result.getString("code").equals("ok")) {
//							JSONObject data = result.getJSONObject("data");
//							if(data != null) {
//								return data;
//							}
//						}
//					}
//					throw new IOException("Url " + url + " content illegal, " + json);
					}
				} else if(code == 208) { //error
					try(InputStream inputStream = connection.getInputStream()) {
						Result result = new Result();
						result.from(inputStream);
						String forId = result.getForId();
						if(forId != null && forId.equals("IOException")) {
							throw new IOException(result.getDescription());
						} else {
							int resultCode = 0;
							if(result.getCode() != null)
								resultCode = result.getCode();
							throw new CoreException(resultCode, result.getDescription());
						}
					}
				} else {
					TapLogger.debug(TAG, "post url {} nodeMessage {} received code {}", url, nodeMessage, code);
				}
			} else {
				throw new IOException("Url(post) occur error, code " + code + " message " + connection.getResponseMessage());
			}
		} finally {
			connection.disconnect();
		}
		return null;
	}

	@Override
	public <Request extends MessageTracker, Response> Response send(String type, Request request, Type responseClass) throws IOException {
		if(!stateMachine.getCurrentState().equals(STATE_READY))
			throw new IOException(FormatUtils.format("NodeConnection's state is not ready, nodeRegistry {}", nodeRegistry));

		if(workableIps.isEmpty())
			throw new IOException(FormatUtils.format("NodeConnection's workableIps is empty, nodeRegistry {}", nodeRegistry));

		Integer port = nodeRegistry.getHttpPort();
		String ip = workableIps.get(0);
		String url = "http://" + ip + ":" + port + "/api/proxy/internal?key=" + ProxyConstants.INTERNAL_KEY;
		String nodeId = CommonUtils.getProperty("tapdata_node_id");
		NodeMessage requestMessage = new NodeMessage()
				.toNodeId(nodeRegistry.id())
				.fromNodeId(nodeId)
				.type(type)
				.encode(Data.ENCODE_JSON)
				.data(toJson(request).getBytes(StandardCharsets.UTF_8))
				.time(System.currentTimeMillis());
		request.requestBytes(requestMessage.getData());
		long time = System.currentTimeMillis();
		NodeMessage responseMessage;
		try {
			responseMessage = post(url, requestMessage);
			request.throwable(null);
		} catch (Throwable throwable) {
			request.throwable(throwable);
			throw throwable;
		} finally {
			request.takes(System.currentTimeMillis() - time);
		}
		if(responseMessage != null) {
			request.responseBytes(responseMessage.getData());
		}
		if(responseMessage != null && responseMessage.getData() != null)
			return fromJson(new String(responseMessage.getData(), StandardCharsets.UTF_8), responseClass);
		return null;
	}

	@Override
	public <Request extends MessageTracker, Response> void sendAsync(String type, Request request, Type responseClass, BiConsumer<Response, Throwable> biConsumer) throws IOException {
		if(!stateMachine.getCurrentState().equals(STATE_READY))
			throw new IOException(FormatUtils.format("NodeConnection's state is not ready, nodeRegistry {}", nodeRegistry));

		if(workableIps.isEmpty())
			throw new IOException(FormatUtils.format("NodeConnection's workableIps is empty, nodeRegistry {}", nodeRegistry));

		String nodeId = CommonUtils.getProperty("tapdata_node_id");
		NodeMessage nodeMessage = new NodeMessage()
				.toNodeId(nodeRegistry.id())
				.fromNodeId(nodeId)
				.type(type)
				.encode(Data.ENCODE_JSON)
				.data(toJson(request).getBytes(StandardCharsets.UTF_8))
				.time(System.currentTimeMillis())
				.biConsumer((BiConsumer<Object, Throwable>) biConsumer)
				.responseClass(responseClass);
		asyncQueue.offer(nodeMessage);
	}

	@Override
	public boolean isReady() {
		return stateMachine.getCurrentState().equals(STATE_READY);
	}

	public int getRetryTimes() {
		return retryTimes;
	}

	@Override
	public long getTouch() {
		return touch;
	}

	@Override
	public void close() {
		synchronized (this) {
			this.notifyAll();
		}
		if(stateMachine != null && !stateMachine.getCurrentState().equals(STATE_TERMINATED)) {
			stateMachine.gotoState(STATE_TERMINATED, FormatUtils.format("Close NodeConnection, nodeRegistry {}", nodeRegistry));
		}
	}

	@Override
	public String getWorkingIpPort() {
		if(workableIps.isEmpty())
			return null;
		return workableIps.get(0) + ":" + nodeRegistry.getHttpPort();
	}

	@Override
	public String getId() {
		return nodeRegistry.id();
	}

	public List<String> getWorkableIps() {
		return workableIps;
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		return DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
				.kv("stateMachine", stateMachine.getCurrentState())
				.kv("nodeRegistry", nodeRegistry)
				.kv("terminateReason", terminateReason)
				.kv("MAX_RETRY", MAX_RETRY)
				.kv("retryTimes", retryTimes)
				.kv("touch", new Date(touch))
				.kv("workableIps", workableIps)
				.kv("asyncQueue", asyncQueue.memory(keyRegex, memoryLevel))
				;
	}
}
