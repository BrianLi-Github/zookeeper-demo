package com.example.demo.simple;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkSimpleClient {

    private ZooKeeper zkClient;
    private static String connectString = "192.168.71.101:2181,192.168.71.102:2181,192.168.71.103:2181";
    private static Integer sessionTimeout = 2000;
    private static final String rootPath = "/";
    private String childName = "app1";

    /** 等待连接建立成功的信号 */
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private Long connectTime;

    @Before
    public void init() throws IOException, InterruptedException {
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
                            zkClient.getChildren(rootPath, true); break;
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

    @Test
    public void getPath() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(rootPath, true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getData() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(rootPath + childName, false);
        if (stat != null) {
            byte[] data = zkClient.getData(rootPath + childName, true, null);
            System.out.println(new String(data));
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        zkClient.create(rootPath + childName, "this is brian's app1.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void modifyData() throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData(rootPath + childName, "Modify app1's data".getBytes(), -1);
        if (stat != null) {
            System.out.println(stat);
        }
    }

    @Test
    public void delNode() throws KeeperException, InterruptedException {
        //version  --> 删除哪个版本  -->  -1表示所有版本
        zkClient.delete(rootPath + childName, -1);
    }
}
