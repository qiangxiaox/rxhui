package com.gemantic.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;

@Component
public class CanalClient implements DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(CanalClient.class);
    private CanalConnector canalConnector;

    @Autowired
    CanalConfigProp configProp;


    @Bean
    public CanalConnector getCanalConnector() {
        if(configProp ==null){
            throw new RuntimeException("ERROR , canal configProp is null");
        }
        String ip = configProp.getIp();

        // 创建链接
        InetSocketAddress inetSocketAddress = new InetSocketAddress(ip, Integer.valueOf(configProp.getPort()));
        canalConnector = CanalConnectors.newClusterConnector(Lists.newArrayList(inetSocketAddress), configProp.getDestination(), configProp.getUsername(), configProp.getPassport());
        // 链接canal
        canalConnector.connect();
        // 指定filter，格式 {database}.{table}，这里不做过滤，过滤操作留给用户
        canalConnector.subscribe("dw.dict_column,dw.dict_table,datacenter_pub.nreport_data_log");
        // 回滚寻找上次中断的位置
        canalConnector.rollback();
        logger.info("canal客户端启动成功");
        return canalConnector;
    }

    /**
     * 当bean销毁时断开canal的链接
     */
    @Override
    public void destroy() {
        if (canalConnector != null) {
            canalConnector.disconnect();
        }
    }
}