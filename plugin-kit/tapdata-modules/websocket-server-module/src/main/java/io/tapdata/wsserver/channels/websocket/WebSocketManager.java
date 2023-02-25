package io.tapdata.wsserver.channels.websocket;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ResourceLeakDetector;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.wsserver.channels.websocket.impl.GatewayHandlerInitializer;
import io.tapdata.wsserver.channels.websocket.impl.WebSocketProperties;

@Bean
public class WebSocketManager {
    private final static String TAG = WebSocketManager.class.getSimpleName();
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    @Bean
    private WebSocketProperties webSocketProperties;

    private boolean started = false;

    private Throwable startFailed;

    public void start() {
        if(!started) {
            started = true;
            try {
                GatewayHandlerInitializer initializer = new GatewayHandlerInitializer(this.webSocketProperties);
                InstanceFactory.injectBean(initializer);
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                        .handler(new LoggingHandler(LogLevel.DEBUG))
                        .option(ChannelOption.SO_BACKLOG, webSocketProperties.getBacklog())
                        .childOption(ChannelOption.TCP_NODELAY, true).childHandler(initializer);
                ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
                bootstrap.bind(webSocketProperties.getPort()).sync();
                TapLogger.debug(TAG, "Websocket server started at port " + webSocketProperties.getPort());
            } catch(Throwable throwable) {
                throwable.printStackTrace();
                throw new CoreException(NetErrors.WEBSOCKET_SERVER_START_FAILED, "Websocket server start failed, {}", throwable.getMessage());
            }
        }
    }

    public void stop() {
        if(started) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public WebSocketProperties getWebSocketProperties() {
        return webSocketProperties;
    }
}
