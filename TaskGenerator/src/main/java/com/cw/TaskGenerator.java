package com.cw;

import com.cw.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * @ClassName : TaskGenerator
 * @Description :  单线程生成id，x，y任务，TCP发送给Executor，利用UDP每分钟上报生成任务个数
 * @Author : Ethan Chan
 * @Date: 2023/5/5 10:26
 */
@Slf4j
public class TaskGenerator {

    /** Performance much faster than Random class */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private static final int BATCH_SIZE = 2048;
    private static final int GENERATOR_TASK_BYTES = 12;
    private DatagramSocket udpSocket;
    private long id;
    private LongAdder nTasksPerMin;
    private long nTasksPerSecond;
    /**
     * 1. volatile 在多写环境下是非线程安全的
     * 2. AtomicLong 在高并发环境下会有多个线程去竞争一个原子变量，而始终只有一个线程能竞争成功，而其他线程会一直通过 CAS 自旋尝试获取此原子变量，因此会有一定的性能消耗
     * 3. LongAdder 会将这个原子变量分离成一个 Cell 数组，每个线程通过 Hash 获取到自己数组，这样就减少了乐观锁的重试次数，从而在高竞争下获得优势
     */
    private ByteBuffer writeBuffer;
    private int mins;
    private Socket tcpSendSocket;
    private InetSocketAddress monitorAddress;
    private DataOutputStream outputStream;
    public TaskGenerator(String toIP, int toPort, String monitorIP, int monitorPort, long nTasksPerSecond) {
        this.nTasksPerSecond = nTasksPerSecond;
        id = 0L;
        mins = 0;
        nTasksPerMin = new LongAdder();
        try {
            writeBuffer = ByteBuffer.allocate(BATCH_SIZE * GENERATOR_TASK_BYTES);
            udpSocket = new DatagramSocket();
            monitorAddress = new InetSocketAddress(monitorIP, monitorPort);

            tcpSendSocket = new Socket(toIP, toPort);
            outputStream = new DataOutputStream(tcpSendSocket.getOutputStream());
            System.out.println("Generator Connected Executor Success");
        } catch (IOException e) {
            Utils.close(udpSocket, outputStream, tcpSendSocket);
            log.error(e.toString());
        }
    }

    private long next() {
        return ++id;
    }

    private int generateNumber() {
        return random.nextInt(65535) + 1;
    }

    public void generateTasks() {
        for (int cnt = 0; cnt < nTasksPerSecond; cnt += BATCH_SIZE) {
            try {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    long id = next();
                    int x = generateNumber();
                    int y = generateNumber();
                    writeBuffer.putLong(id);
                    writeBuffer.putShort((short) (x & 0xffff));  // 通过(short) (x & 0xFFFF)操作得到低16位
                    writeBuffer.putShort((short) (y & 0xffff));
                }
                outputStream.write(writeBuffer.array());
                outputStream.flush();
                writeBuffer.clear();
            } catch (Exception e) {
                log.error("while generating:", e);
            }
            nTasksPerMin.add(BATCH_SIZE);
        }
    }

    private void monitor() {
        try {
            int num = nTasksPerMin.intValue();
            nTasksPerMin.reset();
            String msg = num + " tasks generated";
            System.out.println("------- " + new Date() + "------" + this.mins++ + " mins--------------- \n" + msg);
            byte[] datas = ("GEN@" + msg).getBytes();
            //参数：数据，数据开始点，数据长度，发送的地址
            DatagramPacket packet = new DatagramPacket(datas, 0, datas.length, monitorAddress);
            //3.发送数据包
            udpSocket.send(packet);
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    private void start() {
        ScheduledExecutorService monitorThread = Executors.newSingleThreadScheduledExecutor();
        monitorThread.scheduleAtFixedRate(this::monitor, 0, 60, TimeUnit.SECONDS);
        ScheduledExecutorService generateThread = Executors.newSingleThreadScheduledExecutor();
        generateThread.scheduleAtFixedRate(this::generateTasks, 0, 1, TimeUnit.SECONDS);
        System.out.println("Generator start to work");
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        System.out.println("Please enter the Monitoring System IP address and port, for example 127.0.0.1:9999");
        String[] s = sc.nextLine().split(":");
        String monitorIP = s[0];
        int monitorPort = Integer.parseInt(s[1]);

        System.out.println("Please enter the Task Executor IP address and port, for example 127.0.0.1:7777");
        String[] s1 = sc.nextLine().split(":");
        String taskExecutorIP = s1[0];
        int taskExecutorPort = Integer.parseInt(s1[1]);

        System.out.println("Please enter the amount of tasks generated per second, for example 320000");
        long tasksPerSeconds = sc.nextLong();
        TaskGenerator generator = new TaskGenerator(taskExecutorIP, taskExecutorPort, monitorIP, monitorPort, tasksPerSeconds);
        generator.start();
    }
}
