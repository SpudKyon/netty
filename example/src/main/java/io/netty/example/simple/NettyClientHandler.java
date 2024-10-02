package io.netty.example.simple;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author Spud
 * @date 2024/9/25
 */
public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        byte[] bytes = "hi,服务端，我是客户端！".getBytes();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        ctx.channel().writeAndFlush(byteBuf);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf responseByteBuf = (ByteBuf) msg;
        byte[] responseByte = new byte[responseByteBuf.readableBytes()];
        responseByteBuf.readBytes(responseByte);
        System.out.println(new String(responseByte));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
    }
}
