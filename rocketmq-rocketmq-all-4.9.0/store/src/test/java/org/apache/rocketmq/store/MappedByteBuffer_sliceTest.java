package org.apache.rocketmq.store;
import org.junit.Test;
import java.nio.ByteBuffer;

/**
 * @Author : haiyang.luo
 * @Date : 2022/11/19 22:45
 * @Description :
 */
public class MappedByteBuffer_sliceTest {
    /**
     * Buffer:
     * 1) capacity（容量）； 2) position（读写位置）； 3) limit（读写限制）； 4）mark（标记）；
     * mark临时存储position，方便需要恢复到position时，直接取mark即可。
     *
     * Buffer#slice表示的是从Buffer分片，并从position位置开始进行。slice分配出的Buffer对原Buffer可见。
     *
     */
    @Test
    public void testByteBufferSlice() {
        byte[] bytes = new byte[]{1,2,3,4,5};
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.position(2);
        System.out.println("byteBuffer position:{" + byteBuffer.position() + "} limit:{" + byteBuffer.limit() + "} capacity:{" + byteBuffer.capacity() + "}");
        ByteBuffer sliceByteBuffer = byteBuffer.slice();
        System.out.println("byteBuffer offset:{" + byteBuffer.arrayOffset() + "}");
        System.out.println("sliceByteBuffer position:{" + sliceByteBuffer.position() + "} limit:{" + sliceByteBuffer.limit() + "} capacity:{" + sliceByteBuffer.capacity() + "}");
        sliceByteBuffer.put((byte)66);
        for (int i = 0; i < bytes.length; i++) {
            System.out.print(bytes[i]);
        }
        System.out.println();
        for (int i = 0; i < sliceByteBuffer.capacity(); i++) {
            System.out.print(sliceByteBuffer.get(i));
        }
        System.out.println();
        System.out.println("sliceByteBuffer offset:{" + sliceByteBuffer.arrayOffset() + "}");
        /* 输出结果：
            byteBuffer position:{2} limit:{5} capacity:{5}
            byteBuffer offset:{0}
            sliceByteBuffer position:{0} limit:{3} capacity:{3}
            126645
            6645
            sliceByteBuffer offset:{2}
         */
    }

    /**
     * Buffer有两个模式，读模式和写模式。对于Buffer，写模式不可以读，读模式不可以写。否则会报java.nio.BufferOverflowException异常。
     *
     */
    @Test
    public void testByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        for (int i = 0; i < byteBuffer.capacity(); i++) {
            byteBuffer.put((byte)i);
        }
        byteBuffer.flip();
        for (int i = 0; i < byteBuffer.capacity(); i++) {
            System.out.print(byteBuffer.get());
        }
        // 报异常
//        byteBuffer.put((byte)100);
    }
}
