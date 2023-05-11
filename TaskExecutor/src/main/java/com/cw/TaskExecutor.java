package com.cw;

import com.cw.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;


/**
 * @ClassName : TaskExecutor
 * @Description :  1. TCP接受任务，多线程计算x^y，再循环计算SHA256 10次，用TCP将结果发给Validator。
 * 2. 定时任务UDP每分钟上报周期内完成计算任务数量。
 * @Author : Ethan Chan
 * @Date: 2023/5/5 10:27
 */
@Slf4j
public class TaskExecutor {

    private BlockingQueue<ExecuteTasks> tasksPool;
    private static final int SCHEDULED_THREAD_POOL_SIZE = 1;
    private static final int GENERATOR_TASK_BYTES = 12;
    private static final int TASK_BYTES = 12 + 32;
    private static final int BATCH_SIZE = 2048;
    private final ConcurrentLinkedQueue<byte[]> taskQueue = new ConcurrentLinkedQueue<>();
    private LongAdder nTasksPerMin;
    // 本地计算时间为x，等待时间为y，则工作线程数（线程池线程数）设置为 N*(x+y)/x，能让CPU的利用率最大化。
    private static final int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors() + 1;
    private ThreadPoolExecutor threadPool;
    private DatagramSocket udpSocket;
    private ServerSocket server;
    private Socket socket;
    private String monitorIP;
    private int monitorPort;
    private Socket tcpSendSocket;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;

    public TaskExecutor(int port, String toIP, int toPort, String monitorIP, int monitorPort) {
        this.monitorIP = monitorIP;
        this.monitorPort = monitorPort;
        this.nTasksPerMin = new LongAdder();
        try {
            udpSocket = new DatagramSocket(port);
            server = new ServerSocket(port);
            socket = server.accept();
            inputStream = new DataInputStream(socket.getInputStream());
            tcpSendSocket = new Socket(toIP, toPort);
            outputStream = new DataOutputStream(tcpSendSocket.getOutputStream());
            readBuffer = ByteBuffer.allocate(BATCH_SIZE * GENERATOR_TASK_BYTES);
            writeBuffer = ByteBuffer.allocate(BATCH_SIZE * TASK_BYTES);
            threadPool = new ThreadPoolExecutor(
                    CORE_POOL_SIZE,
                    CORE_POOL_SIZE,
                    60,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(10240),
                    Executors.defaultThreadFactory(),
                    new ThreadPoolExecutor.DiscardPolicy());

            // 对象池
            tasksPool = new LinkedBlockingDeque<>(CORE_POOL_SIZE*32);
            for (int i = 0; i < CORE_POOL_SIZE*8; i++) {
                tasksPool.add(new ExecuteTasks(BATCH_SIZE));
            }
            System.out.println("Executor Connected Validator Success");
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    private void start() {
        monitor();
        threadPool.execute(this::sendTasks2Validator);
        receiveAndHandleTasks();
        System.out.println("Executor start to work");
    }

    private void monitor() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(SCHEDULED_THREAD_POOL_SIZE);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long num = nTasksPerMin.intValue();
                nTasksPerMin.reset();
                String msg = "EXE" + "@" + num + " tasks completed. EXE TPS is " + num / 60.0 + ". Waiting queue: " + threadPool.getQueue().size() + ". Tasks Pool size: " + tasksPool.size();
                Utils.send(udpSocket, monitorIP, monitorPort, msg);
            } catch (IOException e) {
                log.error("UDP sending:" + e.toString());
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    /**
     * @author: Ethan Chan
     * @date: 2023/5/5
     * @Description: 按行读取任务，利用线程池执行
     **/
    public void receiveAndHandleTasks() {
        try {
            for (; ; ) {
                inputStream.readFully(readBuffer.array());
                ExecuteTasks tasks = tasksPool.poll();
                if (tasks == null) {
                    tasks = new ExecuteTasks(BATCH_SIZE);
                }
                for (int i = 0; i < BATCH_SIZE; i++) {
                    long id = readBuffer.getLong();
                    int x = readBuffer.getShort() & 0xffff;
                    int y = readBuffer.getShort() & 0xffff;
                    tasks.putOneTask(id, x, y);
                }
                threadPool.execute(tasks);
                readBuffer.clear();
            }
        } catch (Exception e) {
            log.error("Executor: ", e);
            Utils.close(server, socket, udpSocket);
        }
    }

    public void sendTasks2Validator() {
        for (; ; ) {
            if (taskQueue.isEmpty()) {
                continue;
            }
            byte[] batchTasks = taskQueue.poll();
            writeBuffer.put(batchTasks);
            try {
                outputStream.write(writeBuffer.array());
                outputStream.flush();
                writeBuffer.clear();
                nTasksPerMin.add(BATCH_SIZE);
            } catch (IOException e) {
                log.error("Executor send: ", e);
            }
        }
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int taskExecutorPort = 7777;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            System.out.println("Executor Address: " + addr.getHostAddress() + ":" + taskExecutorPort);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Please enter the Monitoring System IP address and port, for example 127.0.0.1:9999");
        String[] s = sc.nextLine().split(":");
        String monitorIP = s[0];
        int monitorPort = Integer.parseInt(s[1]);

        System.out.println("Please enter the Task Validator IP address and port, for example 127.0.0.1:6666");
        String[] s1 = sc.nextLine().split(":");
        String valIP = s1[0];
        int valPort = Integer.parseInt(s1[1]);
        System.out.println("Executor ready!");
        TaskExecutor executor = new TaskExecutor(taskExecutorPort, valIP, valPort, monitorIP, monitorPort);
        executor.start();
    }

    private class ExecuteTasks implements Runnable {
        private MessageDigest digest;
        private int index;
        private byte[] sha256;
        private long[] ids;
        private int[] xs;
        private int[] ys;
        private ByteBuffer buffer;
        private byte[] bytes;
        public ExecuteTasks(int capacity) {
            index = 0;
            sha256 = new byte[32];
            ids = new long[capacity];
            xs = new int[capacity];
            ys = new int[capacity];
            buffer  = ByteBuffer.allocate(capacity * TASK_BYTES);
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                log.error("MessageDigest: " + e.toString());
            }
        }

        public void putOneTask(long id, int x, int y) {
            ids[index] = id;
            xs[index] = x;
            ys[index] = y;
            index++;
        }

        @Override
        public void run() {
            for (int i = 0; i < BATCH_SIZE; i++) {
                executeOneTask(ids[i], xs[i], ys[i]);
            }
            taskQueue.offer(buffer.array());
            buffer.clear();
            index = 0;
            try {
                tasksPool.put(this);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public byte[] getSha256TenTimes(String s) {
            bytes = s.getBytes();
            for (int i = 0; i < 10; ++i) {
                bytes = digest.digest(bytes);
            }
            return bytes;
        }

        public String fastPow(int x, int y) {
            long res = 1;
            while (y > 0) {
                if ((y & 1) == 1) res *= x; // 二进制最右一位是否为1
                x *= x;
                y >>= 1; // 除以 2
            }
            return String.valueOf(res);
        }

        private void executeOneTask(long id, int x, int y) {
            sha256 = getSha256TenTimes(fastPow(x, y));
            buffer.putLong(id);
            buffer.putShort((short) (x & 0xffff));
            buffer.putShort((short) (y & 0xffff));
            buffer.put(sha256);
        }
    }
}