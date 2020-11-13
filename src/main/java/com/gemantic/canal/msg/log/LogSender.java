package com.gemantic.canal.msg.log;

import com.gemantic.config.RabbitConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.PendingConfirm;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.UUID;

/**
 * createed By xiaoqiang
 * 2020/11/12 11:17
 */
@Slf4j
@Component
public class LogSender {
	@Resource
	private RabbitTemplate rabbitTemplate;

	public void send(Object msg) {
		CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
		log.info("send: " + correlationData.getId());
		this.rabbitTemplate.convertAndSend(RabbitConfig.NORMAL_EXCHANGE, RabbitConfig.LOG_ROUTINGKEY, msg, correlationData);
		this.rabbitTemplate.handleConfirm(new PendingConfirm(correlationData, System.currentTimeMillis()), true);
	}
}
