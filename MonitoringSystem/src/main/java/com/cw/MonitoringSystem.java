package com.cw;

import com.cw.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.UnknownHostException;

import java.net.InetAddress;

@Slf4j
public class MonitoringSystem {

    private enum ActionEnum {
        GEN(), EXE(), VAL();
    }

    private DatagramSocket udpSocket;
    private int byteNum;

    public MonitoringSystem(int port, int byteNum) {
        try {
            udpSocket = new DatagramSocket(port);
            this.byteNum = byteNum;
        } catch (IOException e) {
            log.error("Construct Monitoring System errors:" + e);
            Utils.close(udpSocket);
        }
    }

    public void run() {
        for (; ; ) {
            try {
                byte[] container = new byte[byteNum];
                DatagramPacket packet = new DatagramPacket(container, 0, container.length);
                //阻塞式接受包裹
                udpSocket.receive(packet);
                //显示接受数据
                byte[] datas = packet.getData();
                String data = new String(datas).trim();
                if (!data.equals("")) {
                    String[] s = data.split("@");
                    String actionType = s[0].toUpperCase();
                    String content = s[1];
                    if (Utils.enumContains(ActionEnum.class, actionType)) {
                        udpHandle(ActionEnum.valueOf(actionType), content);
                    }
                }
            } catch (IOException e) {
                log.error(e.toString());
            }
        }
    }

    private void udpHandle(ActionEnum actionType, String content) {
        switch (actionType) {
            case GEN: {
                log.info( "Generator: " + content);
                log.info("------------------------------------------------");
                break;
            }
            case EXE: {
                log.info("Executor: " + content);
                log.info("=================================================");
                break;
            }
            case VAL: {
                log.info("Validator: " +content);
                log.info("*************************************************");
                break;
            }
        }
    }
    public static void main(String[] args) {
        int monitorPort = 9999;
        int byteNum = 1024;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            System.out.println("Monitoring System Address: "+ addr.getHostAddress()+":"+monitorPort);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        MonitoringSystem ms = new MonitoringSystem(monitorPort, byteNum);
        ms.run();
    }
}
