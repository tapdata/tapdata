package io.tapdata.netty.channels.websocket;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ResourceLeakDetector;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.netty.channels.websocket.impl.GatewayHandlerInitializer;
import io.tapdata.netty.channels.websocket.impl.WebSocketProperties;

public class WebSocketManager {
    private final static String TAG = WebSocketManager.class.getSimpleName();
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private WebSocketProperties webSocketProperties;

    private boolean started = false;

    private Throwable startFailed;

    public void start() {
        if(!started) {
            started = true;
            try {
                GatewayHandlerInitializer initializer = new GatewayHandlerInitializer(this.webSocketProperties);
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                        .handler(new LoggingHandler(LogLevel.DEBUG))
                        .option(ChannelOption.SO_BACKLOG, webSocketProperties.getBacklog())
                        .childOption(ChannelOption.TCP_NODELAY, true).childHandler(initializer);
                ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
                bootstrap.bind(webSocketProperties.getPort()).sync();
                TapLogger.info(TAG, "Websocket server started at port " + webSocketProperties.getPort());
            } catch(Throwable throwable) {
                throwable.printStackTrace();
                startFailed = throwable;
            }
        }
    }

    public void stop() {
        if(started) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
