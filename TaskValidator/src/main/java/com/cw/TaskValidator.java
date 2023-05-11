package com.cw;


import com.cw.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @ClassName : TaskValidator
 * @Description :  TCP接受任务，然后验证，UDP定时每分钟上报任务数量，抽样一百个的正确和错误个数。
 * @Author : Ethan Chan
 * @Date: 2023/5/5 10:24
 */
@Slf4j
public class TaskValidator {
    private static final int SCHEDULED_THREAD_POOL_SIZE = 1;
    private static final int TASK_BYTES = 12 + 32;
    private static final int BATCH_SIZE = 2048;
    private static final double SAMPLE_RATE = 0.005;
    private Random random;
    private DatagramSocket udpSocket;
    private ServerSocket server;
    private Socket socket;
    private int sampleCnt;
    private int rightCnt;
    private int wrongCnt;
    private final String monitorIP;
    private final int monitorPort;
    private ByteBuffer readBuffer;
    private DataInputStream inputStream;
    private final AtomicInteger totalTasksSize;

    public TaskValidator(int port, String monitorIP, int monitorPort) {
        this.monitorIP = monitorIP;
        this.monitorPort = monitorPort;
        this.rightCnt = 0;
        this.wrongCnt = 0;
        this.sampleCnt = 0;
        this.totalTasksSize = new AtomicInteger(0);
        this.random = new Random();
        try {
            udpSocket = new DatagramSocket();
            server = new ServerSocket(port);
            socket = server.accept();
            inputStream = new DataInputStream(socket.getInputStream());
            readBuffer = ByteBuffer.allocate(BATCH_SIZE * TASK_BYTES);
            System.out.println("Validator are starting!");
        } catch (IOException e) {
            log.error("TaskValidator: ", e);
        }
    }

    private String getMonitorMsg(int totalTasksSize) {
        return "VAL" + "@" + new Date() + " total tasks:  " + totalTasksSize + ", after sampling " + this.rightCnt + " tasks are correct while " + this.wrongCnt + " are wrong";
    }

    private void monitor() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(SCHEDULED_THREAD_POOL_SIZE);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                Utils.send(udpSocket, monitorIP, monitorPort, this.getMonitorMsg(totalTasksSize.getAndSet(0)));
                wrongCnt = 0;
                rightCnt = 0;
                sampleCnt = 0;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    public void receiveTasks() {
        try {
            monitor();
            for (; ; ) {
                byte[] res = new byte[32];
                inputStream.readFully(readBuffer.array());
                totalTasksSize.addAndGet(BATCH_SIZE);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    long id = readBuffer.getLong();
                    int x = readBuffer.getShort() & 0xffff;
                    int y = readBuffer.getShort() & 0xffff;
                    readBuffer.get(res);
                    if (sampleCnt < 100 && random.nextDouble() < SAMPLE_RATE) {
                        valTask(id, x, y, res);
                        sampleCnt++;
                    }
                }
                readBuffer.clear();
            }

        } catch (Exception e) {
            log.error("Executor: ", e);
            Utils.close(server, socket, udpSocket);
        }
    }

    private void valTask(long id, int x, int y, byte[] res) {
        try {
            String s = Utils.fastPow(x, y);
            byte[] valRes = Utils.getSha256TenTimes(s);
            String exeRes = Utils.getRes(res);
            String calRes = Utils.getRes(valRes);
            boolean isEquals = exeRes.equals(calRes);
            if (isEquals) {
                rightCnt++;
            } else {
                wrongCnt++;
            }
            log.info("id {} x {} y {} sha256 {} val result {} is correct {}", id, x, y, exeRes, calRes, isEquals);
        } catch (Exception e) {
            log.error("val getting sha256: ", e);
        }
    }


    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int taskValidatorPort = 6666;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            System.out.println("Validator Address: " + addr.getHostAddress() + ":" + taskValidatorPort);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Please enter the Monitoring System IP address and port, for example 127.0.0.1:9999");
        String[] s = sc.nextLine().split(":");
        String monitorIP = s[0];
        int monitorPort = Integer.parseInt(s[1]);
        System.out.println("Validator ready!");
        TaskValidator validator = new TaskValidator(taskValidatorPort, monitorIP, monitorPort);
        validator.receiveTasks();
    }
}
