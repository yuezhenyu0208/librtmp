package com.tsingtech.librtmp;

import cn.hutool.core.io.FileUtil;
import com.github.faucamp.simplertmp.io.ChunkStreamInfo;
import com.tsingtech.librtmp.config.RtmpProperties;
import com.tsingtech.librtmp.handler.RtmpClientInitializer;
import com.tsingtech.librtmp.handler.SrsFlvMuxer;
import com.tsingtech.librtmp.handler.SrsFlvMuxer2;
import com.tsingtech.librtmp.util.BeanUtil;
import com.tsingtech.librtmp.vo.DataPacket;
import com.tsingtech.librtmp.vo.VideoPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Author: chrisliu
 * Date: 2019/8/25 11:03
 * Mail: gwarmdll@gmail.com
 * 白茶清欢无别事，我在等风也等你
 */
public final class RtmpClient {
    static String hostR = "push.kakahui.net";
    static int portT = 1935;
    private ConcurrentLinkedQueue<DataPacket> avQueue;

    public String getStreamName() {
        return streamName;
    }

    private String streamName;

    private String app;

    private String host;

    private int port;

    private String tcUrl;

    private Bootstrap bootstrap;

    /** Default chunk size is 128 bytes */
    private int rxChunkSize = 128;
    private int txChunkSize = 128;
    private Map<Integer, ChunkStreamInfo> chunkChannels = new HashMap<Integer, ChunkStreamInfo>();
    private Map<Integer, String> invokedMethods = new ConcurrentHashMap<Integer, String>();

    private SrsFlvMuxer2 srsFlvMuxer;
    private SrsFlvMuxer flvMuxer;
    private boolean inited = false;
    public ChannelFuture channelFuture;


    private RtmpClient (Builder builder) {
        bootstrap = new Bootstrap();
        this.avQueue = new ConcurrentLinkedQueue<>();
        this.srsFlvMuxer = new SrsFlvMuxer2();
        //ChannelHandlerContext ctx = builder.ctx;
        EventLoopGroup worker = new NioEventLoopGroup();
        bootstrap.group(worker).channel(NioSocketChannel.class).handler(new RtmpClientInitializer(this));

        try {
            channelFuture = bootstrap.connect(new InetSocketAddress(InetAddress.getByName(builder.host), builder.port));
            channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    System.out.println("链接成功");
                    this.inited = true;
                    this.streamName = builder.streamName;
                    //RtmpProperties rtmpProperties = BeanUtil.getBean(RtmpProperties.class);
                    this.app = builder.app ;
                    this.host = builder.host;
                    this.port = builder.port;


                    //StringBuilder tcBuilder = new StringBuilder("rtmp://");
                    StringBuilder tcBuilder = new StringBuilder("rtmp://push.kakahui.net/live/1111?txSecret=124b4f1056b12d6a229719da5736b90c&txTime=5F548027");
                    tcBuilder.append(this.host).append(":").append(this.port).append("/").append(this.app);
                    this.tcUrl = tcBuilder.toString();
                    System.out.println(this.tcUrl);
                    System.out.println("链接状态"+ channelFuture.channel().isOpen());
                } else {
                    System.err.println("Bind attempt failed!");
                    future.cause().printStackTrace();
                    //ctx.channel().close();
                }
            });

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.println("完成");
    }

    public static void main(String[] args) throws Exception {
        try {
            RtmpClient rtmpClient = Builder.createInstance().setHost("push.kakahui.net").setPort(1935).setApp("live111").setStreamName("1111?txSecret=fde333dae2ccb2430716d6310252fb31&txTime=5F9C0077").build();
            while (!rtmpClient.channelFuture.channel().isActive()){

            }
            Thread.sleep(5000L);
            DataPacket msg = buildDataPacket();
            //rtmpClient.channelFuture.channel().writeAndFlush("sg.getBody()");
            rtmpClient.publish3(msg);

            while (true){
                Thread.sleep(1000L);
                System.out.println("Publish.Start:"+rtmpClient.channelFuture.channel().isRegistered());
                System.out.println("Publish.Start:"+rtmpClient.channelFuture.channel().isOpen());
                System.out.println("Publish.Start:"+rtmpClient.channelFuture.channel().isActive());
            }
            //RtmpClient rtmpClient = Builder.createInstance().setHost("push.kakahui.net").setPort(1935).setApp("live111").setStreamName("1111").build();
            /*Thread.sleep(10000);
            System.out.println("rtmpClient 初始化结果"+rtmpClient.inited);
            //RtmpClient rtmpClient = Builder.createInstance().setHost("push.kakahui.net").setPort(1935).build();

            DataPacket msg = new DataPacket();
            File file = new File("/home/yue/Videos/test.webm");
            byte[] by = FileUtil.readBytes(file);
            ByteBuf buf = Unpooled.wrappedBuffer(by);
            msg.setBody(buf);

            rtmpClient.publish3(msg);
            System.out.println("推流完成");*/
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private static DataPacket buildDataPacket(){
        DataPacket msg = new VideoPacket();
        File file = new File("/home/yue/Videos/test.webm");
        //File file = new File("/home/yue/Videos/advert.mp4");
        //File file = new File("/home/yue/Videos/dump.webm");
        byte[] by = FileUtil.readBytes(file);
        ByteBuf buf = Unpooled.wrappedBuffer(by);
        msg.setBody(buf);
        msg.setTimestamp(System.currentTimeMillis());
        return msg;
    }
    public void setInited () {
        inited = true;
    }

    public ChunkStreamInfo getChunkStreamInfo(int chunkStreamId) {
        ChunkStreamInfo chunkStreamInfo = chunkChannels.get(chunkStreamId);
        if (chunkStreamInfo == null) {
            chunkStreamInfo = new ChunkStreamInfo();
            chunkChannels.put(chunkStreamId, chunkStreamInfo);
        }
        return chunkStreamInfo;
    }

    public String takeInvokedCommand(int transactionId) {
        return invokedMethods.remove(transactionId);
    }

    public String addInvokedCommand(int transactionId, String commandName) {
        return invokedMethods.put(transactionId, commandName);
    }

    public int getRxChunkSize() {
        return rxChunkSize;
    }

    public void setRxChunkSize(int chunkSize) {
        this.rxChunkSize = chunkSize;
    }

    public int getTxChunkSize() {
        return txChunkSize;
    }

    public void setTxChunkSize(int chunkSize) {
        this.txChunkSize = chunkSize;
    }

    public void publish3 (DataPacket msg) throws Exception {
        System.out.println("推流时链接状态"+ channelFuture.channel().isOpen());
        this.srsFlvMuxer.write(channelFuture.channel(), msg);
    }

    @Data
    @Accessors(chain = true)
    public static class Builder {
        private String streamName;

        private String app;

        private String host;

        private Integer port;

        private ChannelHandlerContext ctx;

        public static Builder createInstance () {
            return new Builder();
        }

        public RtmpClient build() {
            return new RtmpClient(this);
        }
    }
}
