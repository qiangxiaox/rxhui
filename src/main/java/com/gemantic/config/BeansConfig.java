package com.gemantic.config;

import com.gemantic.springcloud.utils.SpringBeanUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;

/**
 * 配置bean
 *
 * @author yhye 2016年9月20日下午4:55:17
 */
@Configuration
public class BeansConfig {
    private @Resource Environment environment;

    public @Bean
    SpringBeanUtil springBeanUtil() {
        return new SpringBeanUtil();
    }

    public @Bean RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setReadTimeout(environment.getProperty("client.http.request.readTimeout", Integer.class, 15000));
        requestFactory.setConnectTimeout(environment.getProperty("client.http.request.connectTimeout", Integer.class, 3000));
        RestTemplate rt = new RestTemplate(requestFactory);
        return rt;
    }

}
