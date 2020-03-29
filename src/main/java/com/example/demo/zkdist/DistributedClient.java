package com.example.demo.zkdist;

import org.apache.zookeeper.*;
import org.assertj.core.util.Lists;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class DistributedClient {

    private ZooKeeper zkClient;
    private final static String connectString = "192.168.71.101:2181,192.168.71.102:2181,192.168.71.103:2181";
    private Integer sessionTimeout = 2000;

    private String parentNode = "/servers";

    /** 等待连接建立成功的信号 */
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private Long connectTime;

    private volatile List<ServerInfo> serverList;

    //获取zookeeper集群连接
    public void getConnection() throws Exception {
        connectTime = System.currentTimeMillis();
        System.out.println("开始连接zookeeper, time: " + connectTime);
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
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
                        case NodeChildrenChanged:  // for get path
                            getServerList();
                            break;
                        case NodeDataChanged:  // for get data
                            zkClient.getData(watchedEvent.getPath(), true, null);break;
                        default:break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        connectedSemaphore.await();
    }

    //  获取server list，并监听节点变化
    public void getServerList() throws KeeperException, InterruptedException {
        List<ServerInfo> serverInfos = Lists.newArrayList();
        List<String> children = zkClient.getChildren(parentNode, true);
        if (!CollectionUtils.isEmpty(children)) {
            for (String nodeName : children) {
                byte[] data = zkClient.getData(parentNode + "/" + nodeName, true, null);
                Optional<ServerInfo> objData = ByteArrayUtils.bytesToObject(data);
                serverInfos.add(objData.get());
            }
        }
        serverList = serverInfos;
        System.out.println("最新的服务器列表是: " + serverList);
    }

    public void handleBusinesses() throws KeeperException, InterruptedException {
        if (CollectionUtils.isEmpty(serverList)) {
            System.out.println("没有获取到可用的服务器");
        } else {
            ServerInfo serverInfo = serverList.get(0);
            System.out.println("获取到服务器，正在进行业务处理， 服务器信息： " + serverInfo);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        DistributedClient client = new DistributedClient();
        //1. 获取连接
        client.getConnection();

        //2. 获取最新的server list
        client.getServerList();

        //3. 从server list中获取一个server处理业务
        client.handleBusinesses();
    }
}
