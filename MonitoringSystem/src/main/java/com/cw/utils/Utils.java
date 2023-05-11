package com.cw.utils;

import lombok.Data;

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

    public static int shortByte2Int(byte[] res) {
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(res));
        int a = 0;
        try {
            a = dataInputStream.readUnsignedShort();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return a;
    }

    public static void send(DatagramSocket socket, String toIP, int toPort, String msg) throws IOException {
        byte[] datas = msg.getBytes();
        //参数：数据，数据开始点，数据长度，发送的地址
        DatagramPacket packet = new DatagramPacket(datas, 0, datas.length, new InetSocketAddress(toIP, toPort));
        //3.发送数据包
        socket.send(packet);
    }

    public static String getSha256TenTimes(String s) throws NoSuchAlgorithmException {
        MessageDigest messageDigest;
        StringBuilder res;
        byte[] bytes = s.getBytes();
        messageDigest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < 10; ++i) {
            bytes = messageDigest.digest(bytes);
        }
        res = new StringBuilder();
        for (byte b : bytes) {
            // 1.取出字节b的高四位的数值并追加
            // 把高四位向右移四位，与 0x0f运算得出高四位的数值
            res.append(HEX.charAt((b >> 4) & 0x0f));
            // 2.取出低四位的值并追加
            // 直接与 0x0f运算得出低四位的数值
            res.append(HEX.charAt(b & 0x0f));
        }
        return res.toString();
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
}