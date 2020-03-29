package com.example.demo.zkdist;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * 服务端
 * 启动时往zookeeper注册节点
 */
public class DistributedServer {

    private ZooKeeper zkServer;
    private final static String connectString = "192.168.71.101:2181,192.168.71.102:2181,192.168.71.103:2181";
    private Integer sessionTimeout = 2000;

    private String parentNode = "/servers";

    /** 等待连接建立成功的信号 */
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private Long connectTime;

    //获取zookeeper集群连接
    public void getConnection() throws Exception {
        connectTime = System.currentTimeMillis();
        System.out.println("开始连接zookeeper, time: " + connectTime);
        zkServer = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("Node change: " + watchedEvent.getType() + "---" + watchedEvent.getPath());
                try {
                    Event.EventType eventType = watchedEvent.getType();
                    switch (eventType) {
                        case None:
                            // 连接状态发生变化
                            if (Event.KeeperState.SyncConnected.equals(watchedEvent.getState())) {
                                long endTime = System.currentTimeMillis();
                                System.out.println("zookeeper 连接成功， 耗时: " + (endTime - connectTime) + "毫秒");
                                connectedSemaphore.countDown();
                            }
                            break;
                        default:break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        connectedSemaphore.await();
        if (null == zkServer.exists(parentNode, false)) {
            zkServer.create(parentNode, "ZKServerList".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
    
    public boolean registerServer(String hostname) throws KeeperException, InterruptedException {
        ServerInfo serverInfo = new ServerInfo(hostname, "192.168.71.101", 8080);
        String serverPath = zkServer.create(parentNode + "/server" + hostname, ByteArrayUtils.objectToBytes(serverInfo).get(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        if (serverPath == null || "".equals(serverPath)) {
            return false;
        }
        System.out.println("Server " + hostname + " is online, and the node path is: " + serverPath);
        return true;
    }
    
    public void handleBusinesses(String hostname) throws InterruptedException {
        System.out.println(hostname + " start working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        DistributedServer server = new DistributedServer();

        //1. 获取zookeeper连接
        server.getConnection();

        //2. 向zookeeper集群注册服务器信息
        server.registerServer(args[0]);

        //3. 处理业务逻辑
        server.handleBusinesses(args[0]);
    }
}
