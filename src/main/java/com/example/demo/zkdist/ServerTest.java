package com.example.demo.zkdist;

public class ServerTest {

    public static void main(String[] args) throws Exception {
        DistributedServer server = new DistributedServer();
        //1. 获取zookeeper连接
        server.getConnection();

        String serverName = "shizhan02";
        //2. 向zookeeper集群注册服务器信息
        server.registerServer(serverName);

        //3. 处理业务逻辑
        server.handleBusinesses(serverName);
    }
}
