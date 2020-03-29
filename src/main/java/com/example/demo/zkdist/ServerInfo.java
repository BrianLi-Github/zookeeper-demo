package com.example.demo.zkdist;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ServerInfo implements Serializable {

    private String serverName;
    private String ip;
    private Integer port;


}
