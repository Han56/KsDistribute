package service;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.*;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.group.DefaultChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.*;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GlobalEventExecutor;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

/**
 * @author han56
 * @description 功能描述
 * @create 2022/5/20 上午10:11
 */
public class NettyHttpProxyServer {

    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    //配置类
    private final Configuration configuration;

    //端口号
    private final int port;

    private AsyncConnection connection;

    private Channel channel;

    private ChannelGroup channelGroup;

    //类加载时 初始化参数
    public NettyHttpProxyServer(Configuration configuration,int port) {
        this.configuration = configuration;
        this.port = port;
    }

    //内部参数类
    private static final class Params{
        private final String table;

        private final String row;

        private final String family;

        private final String qualifier;

        public Params(String table, String row, String family, String qualifier) {
            this.table = table;
            this.row = row;
            this.family = family;
            this.qualifier = qualifier;
        }
    }

    //请求处理器
    private static final class RequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final AsyncConnection connection;

        private final ChannelGroup channelGroup;

        public RequestHandler(AsyncConnection connection,ChannelGroup channelGroup){
            this.connection = connection;
            this.channelGroup = channelGroup;
        }

        //返回状态码方法
        private void write(ChannelHandlerContext ctx, HttpResponseStatus status){
            write(ctx,status,null);
        }

        //返回状态码方法
        private void write(ChannelHandlerContext ctx,HttpResponseStatus status,String content){
            DefaultFullHttpResponse resp;
            if (content!=null){
                ByteBuf buf = ctx.alloc().buffer().writeBytes(Bytes.toBytes(content));
                resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,status,buf);
                resp.headers().set(HttpHeaderNames.CONTENT_LENGTH,buf.readableBytes());
            }else {
                resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,status);
            }
            resp.headers().set(HttpHeaderNames.CONTENT_TYPE,"test-plain;charset=UTF-8");
            ctx.writeAndFlush(resp);
        }

        //整理参数
        private Params parse(FullHttpRequest req){
            String[] components = new QueryStringDecoder(req.uri()).path().split("/");
            Preconditions.checkArgument(components.length==4,"Unrecognized uri:%s",req.uri());
            //如果路径开头就是一个 / 因此分割时会得到一个空组件
            String[] cfAndCq = components[3].split(":");
            Preconditions.checkArgument(cfAndCq.length==2,"Unrecognized uri:%s",req.uri());
            return new Params(components[1],components[2],cfAndCq[0],cfAndCq[1]);
        }
        //读取方法  http://localhost:port/{tableName}/{rowKey}/{family}:{cell/qualifier}
        private void get(ChannelHandlerContext ctx,FullHttpRequest request){
            //解析连接参数
            Params params = parse(request);
            addListener(
                    connection.getTable(TableName.valueOf(params.table)).get(new Get(Bytes.toBytes(params.row))
                            .addColumn(Bytes.toBytes(params.family),Bytes.toBytes(params.qualifier))),
                    (res,error)->{
                        if (error!=null){
                            try {
                                exceptionCaught(ctx,error);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }else {
                            byte[] value = res.getValue(Bytes.toBytes(params.family),Bytes.toBytes(params.qualifier));
                            if (value!=null){
                                write(ctx,HttpResponseStatus.OK,Bytes.toStringBinary(value));
                            }else {
                                write(ctx,HttpResponseStatus.NOT_FOUND);
                            }
                        }
                    });
        }

        //写入数据
        private void put(ChannelHandlerContext ctx,FullHttpRequest request){
            //System.out.println("正在存储");
            Params params = parse(request);
            byte[] value = new byte[request.content().readableBytes()];
            request.content().readBytes(value);
            addListener(
                    connection.getTable(TableName.valueOf(params.table)).put(new Put(Bytes.toBytes(params.row))
                            .addColumn(Bytes.toBytes(params.family),Bytes.toBytes(params.qualifier),value)),
                    (res,error)->{
                        if (error!=null){
                            try {
                                exceptionCaught(ctx,error);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }else {
                            write(ctx,HttpResponseStatus.OK);
                        }
                    }
            );
            //System.out.println("存储完毕");
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) throws Exception {
            switch (fullHttpRequest.method().name()){
                case "GET":
                    get(channelHandlerContext,fullHttpRequest);
                    break;
                case "PUT":
                    put(channelHandlerContext,fullHttpRequest);
                    break;
                case "POST":
                    //这里将支持post请求，来优化 get 方法数据量不足的问题
                    System.out.println("post方法测试");
                    break;
                default:
                    write(channelHandlerContext,HttpResponseStatus.METHOD_NOT_ALLOWED);
                    break;
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channelGroup.add(ctx.channel());
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            channelGroup.remove(ctx.channel());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof IllegalArgumentException)
                write(ctx,HttpResponseStatus.BAD_REQUEST,cause.getMessage());
            else
                write(ctx,HttpResponseStatus.INTERNAL_SERVER_ERROR, Throwables.getStackTraceAsString(cause));
        }
    }

    //开启触发器
    private void start() throws ExecutionException, InterruptedException {
        NettyRpcClientConfigHelper.setEventLoopConfig(configuration,workerGroup, NioSocketChannel.class);
        connection = ConnectionFactory.createAsyncConnection(configuration).get();
        channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        channel = new ServerBootstrap().group(bossGroup,workerGroup)
                .channel(NioServerSocketChannel.class).childOption(ChannelOption.TCP_NODELAY,true)
                .childOption(ChannelOption.SO_REUSEADDR,true)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        channel.pipeline().addFirst(new HttpServerCodec(),new HttpObjectAggregator(4*1024*1024),
                                new RequestHandler(connection,channelGroup));
                    }
                }).bind(port).syncUninterruptibly().channel();
    }

    //加入频道
    public void join(){
        channel.closeFuture().awaitUninterruptibly();
    }

    public int port(){
        if (channel==null)
            return port;
        else
            return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    public void stop() throws IOException {
        channel.close().syncUninterruptibly();
        channel=null;
        channelGroup.close().syncUninterruptibly();
        channelGroup=null;
        connection.close();
        connection=null;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration configuration;
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        NettyHttpProxyServer proxyExample = new NettyHttpProxyServer(configuration,45889);
        proxyExample.start();
        proxyExample.join();
    }

}
