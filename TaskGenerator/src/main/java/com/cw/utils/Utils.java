package com.cw.utils;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Utils {

    static final String HEX = "0123456789abcdef";

    public static void close(Closeable... targets) {
        // Closeable是IO流中接口，"..."可变参数
        // IO流和Socket都实现了Closeable接口，可以直接用
        for (Closeable target : targets) {
            try {
                // 只要是释放资源就要加入空判断
                if (null != target) {
                    target.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static byte[] short2Bytes(int num) {
        byte[] bytes = new byte[2];


        for (int i = 0; i < 2; i++) {
            int offset = (2 - 1 - i) * 8;
            bytes[i] = (byte) ((num >>> offset) & 0xff);
        }
        return bytes;
    }

    /**
     * 以字节数组的形式返回指定的 64 位有符号整数值
     *
     * @param data 要转换的数字
     * @return 长度为 8 的字节数组
     */
    public static byte[] long2Bytes(long data) {
        byte[] bytes = new byte[8];
        bytes[7] = (byte) (data & 0xff);
        bytes[6] = (byte) ((data >> 8) & 0xff);
        bytes[5] = (byte) ((data >> 16) & 0xff);
        bytes[4] = (byte) ((data >> 24) & 0xff);
        bytes[3] = (byte) ((data >> 32) & 0xff);
        bytes[2] = (byte) ((data >> 40) & 0xff);
        bytes[1] = (byte) ((data >> 48) & 0xff);
        bytes[0] = (byte) ((data >> 56) & 0xff);
        return bytes;
    }

    public static byte[] task2Bytes(long id, int x, int y, byte[] sha256) {
        byte[] bytes = new byte[8 + 2 + 2 + 32];
        for (int i = 0; i < 8; i++) {
            bytes[7 - i] = (byte) ((id >> (i * 8)) & 0xff);
        }
        for (int i = 0; i < 2; i++) {
            int offset = (2 - 1 - i) * 8;
            bytes[i + 8] = (byte) ((x >>> offset) & 0xff);
        }
        for (int i = 0; i < 2; i++) {
            int offset = (2 - 1 - i) * 8;
            bytes[i + 10] = (byte) ((y >>> offset) & 0xff);
        }
        System.arraycopy(sha256, 0, bytes, 12, 32);
        return bytes;
    }

    public static byte[] getSha256TenTimes(String s) throws NoSuchAlgorithmException {
        MessageDigest messageDigest;
        StringBuilder res;
        byte[] bytes = s.getBytes();
        messageDigest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < 10; ++i) {
            bytes = messageDigest.digest(bytes);
        }
        return bytes;
    }


    public static String fastPow(int x, int y) {
        long res = 1;
        while (y > 0) {
            if ((y & 1) == 1) res *= x; // 二进制最右一位是否为1
            x *= x;
            y >>= 1; // 除以 2
        }
        return String.valueOf(res);
    }


    public static <T extends Enum<T>> boolean enumContains(Class<T> enumerator, String value) {
        for (T c : enumerator.getEnumConstants()) {
            if (c.name().equals(value)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        byte[] bytes0 = long2Bytes(23L);
        System.out.println(Arrays.toString(bytes0));
        byte[] bytes1 = short2Bytes(452);
        System.out.println(Arrays.toString(bytes1));
        byte[] bytes2 = short2Bytes(65534);
        System.out.println(Arrays.toString(bytes2));

        ByteBuffer writeBuffer = ByteBuffer.allocate(12);
        writeBuffer.putLong(23L);
        writeBuffer.putShort((short) (452 & 0xffff));
        writeBuffer.putShort((short) (65534 & 0xffff));
        System.out.println(Arrays.toString(writeBuffer.array()));
        String s = Utils.fastPow(452, 65534);
        byte[] sha256 = getSha256TenTimes(s);
        byte[] bytes = task2Bytes(23L, 452, 65534, sha256);
        System.out.println(Arrays.toString(bytes));
    }
}