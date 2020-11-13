package com.gemantic.canal.msg.delay;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.config.RabbitConfig;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * createed By xiaoqiang
 * 2020/11/11 16:28
 */
@Slf4j
@Component
public class DelayListener implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @RabbitListener(queues = RabbitConfig.DLX_QUEUE)
    public void process(CanalEvent entry, Channel channel, Message message) throws IOException {
        try {
            // 框架容器，是否开启手动ack按照框架配置
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            byte[] byteSource = entry.getByteSource();
            int count = entry.getCount();
            CanalEntry.Entry parseEntry = CanalEntry.Entry.parseFrom(byteSource);
            InsertCanalEvent insertCanalEvent = new InsertCanalEvent(parseEntry);
            insertCanalEvent.setCount(++ count);
            applicationContext.publishEvent(insertCanalEvent);
        } catch (Exception e) {
            //丢弃这条消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
            log.info("处理消息错误-->{}", e);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
