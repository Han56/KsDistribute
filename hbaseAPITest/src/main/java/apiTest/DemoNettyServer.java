package apiTest;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author han56
 * @description 功能描述 Netty TCP服务 入门实例 服务端
 * @create 2022/5/1 上午10:46
 */
public class DemoNettyServer {

    public static void main(String[] args) throws InterruptedException {
        /*
        * 创建 BossGroup 和 WorkerGroup 线程组
        * Boss只处理连接请求  Wroker负责真正处理客户端的需求
        * 两个线程组都是无限循环
        * Boss和Worker含有的子线程（NioEventLoop）数量 默认是cpu数*2
        * */
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);

        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            //创建服务端的启动对象，配置参数
            ServerBootstrap bootstrap = new ServerBootstrap();
            //使用链式编程进行配置
            bootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)//使用NioSocketChannel作为服务器的通道实现
                    .option(ChannelOption.SO_BACKLOG,128)//设置线程队列得到连接个数
                    .childOption(ChannelOption.SO_KEEPALIVE,true)//设置保持活动连接状态
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        //把Handler处理器塞进通道里
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new DemoNettyServerHandler());
                        }
                    });
            System.out.println("服务器 is ready...");
            //绑定一个端口并且同步，生成一个 ChannelFuture对象
            //启动服务器（并绑定端口）
            ChannelFuture channelFuture = bootstrap.bind(6668).sync();
            //对关闭通道进行监听
            channelFuture.channel().closeFuture().sync();
        }finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }


    }

}
