package com.gemantic.canal.msg.delay;

import com.gemantic.config.RabbitConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.UUID;

/**
 * createed By xiaoqiang
 * 2020/11/11 16:28
 */
@Slf4j
@Component
public class DelaySender {
	@Resource
	private RabbitTemplate rabbitTemplate;

	public void send(Object msg) {
		CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
		log.info("send: " + correlationData.getId());
		this.rabbitTemplate.convertAndSend(RabbitConfig.NORMAL_EXCHANGE, RabbitConfig.NORMAL_ROUTINGKEY, msg, correlationData);
	}
}
