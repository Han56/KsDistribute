package apiTest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang.CharSetUtils;

import java.nio.charset.StandardCharsets;

/**
 * @author han56
 * @description 功能描述：每个通道的Handler即处理业务的类
 * @create 2022/5/1 上午11:02
 */
public class DemoNettyServerHandler extends ChannelInboundHandlerAdapter {

    /*
    * 注意：我们自定义的一个Handler需要继承Netty规定好的某个HandlerAdapter（规范）
    * */

    //读取数据（此demo中读取的是客户端发送来的消息）
    /*
    * 1.ChannelHandlerContext ctx:上下文对象，含有管道 pipline 通道 channel 地址
    * 2.Object msg:就是客户端发送的数据 默认是Object
    * */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("server 读取线程："+Thread.currentThread().getName());
        System.out.println("server ctx:"+ctx);
        System.out.println("上述的信息是 channel 和 pipeline 的关系");
        Channel channel = ctx.channel();
        ChannelPipeline channelPipeline = ctx.pipeline();
        //将msg转换为ByteBuf类型
        ByteBuf buf = (ByteBuf) msg;
        System.out.println("client 发送的信息是："+buf.toString(StandardCharsets.UTF_8));
        System.out.println("client 地址："+channel.remoteAddress());
    }

    /*
    * 数据读取完成
    * */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
       // 将数据写入到缓存，并刷新 一般对这个发送的数据进行编码
        ctx.writeAndFlush("hello 客户端", (ChannelPromise) CharsetUtil.UTF_8);
    }

    /*
    * 异常处理 ，一般需要关闭通道
    * */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
