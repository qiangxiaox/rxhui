package com.gemantic.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "canal")
@Data
public class CanalConfigProp {

    private String ip;
    private int port;
    private String destination;
    private String username;
    private String passport;
}