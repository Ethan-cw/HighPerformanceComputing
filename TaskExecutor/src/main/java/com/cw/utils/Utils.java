package com.cw.utils;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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

    public static void send(DatagramSocket socket, String toIP, int toPort, String msg) throws IOException {
        byte[] datas = msg.getBytes();
        //参数：数据，数据开始点，数据长度，发送的地址
        DatagramPacket packet = new DatagramPacket(datas, 0, datas.length, new InetSocketAddress(toIP, toPort));
        //3.发送数据包
        socket.send(packet);
    }

    public static <T extends Enum<T>> boolean enumContains(Class<T> enumerator, String value) {
        for (T c : enumerator.getEnumConstants()) {
            if (c.name().equals(value)) {
                return true;
            }
        }
        return false;
    }
}