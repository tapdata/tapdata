package io.tapdata.wsserver.channels.websocket.impl;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.tapdata.entity.annotations.Bean;

import java.util.concurrent.TimeUnit;


public class GatewayHandlerInitializer extends ChannelInitializer<SocketChannel> {

    @Bean
    private WebSocketProperties nettyProperties;

    private SslContext sslContext;

    public GatewayHandlerInitializer(WebSocketProperties properties) throws Exception {
        this.nettyProperties = properties;
        if (this.nettyProperties.isSsl()) {
            this.createSSL();
        }
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (this.nettyProperties.isSsl() && this.sslContext != null) {
            pipeline.addLast(this.sslContext.newHandler(socketChannel.alloc()));
        }
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(50 * 1024 * 1024));
        pipeline.addLast(new IdleStateHandler(this.nettyProperties.getReadIdleTime(),
                this.nettyProperties.getWriteIdleTime(), this.nettyProperties.getAllIdleTime(), TimeUnit.MINUTES));
        GatewayHandler gatewayHandler = new GatewayHandler(this.nettyProperties.isSsl());
        pipeline.addLast(gatewayHandler);
    }

    private void createSSL() throws Exception {
//        KeyStore ks = KeyStore.getInstance("JKS");
//        InputStream ksInputStream = new FileInputStream(ResourceUtils.getFile("classpath:gateserver.jks"));
//        ks.load(ksInputStream, "123456".toCharArray());
//        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
//        kmf.init(ks, "123456".toCharArray());
//        this.sslContext = SslContextBuilder.forServer(kmf).clientAuth(ClientAuth.NONE).build();
    }

}
