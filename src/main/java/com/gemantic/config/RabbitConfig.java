package com.gemantic.config;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * createed By xiaoqiang
 * 2020/11/11 16:05
 */
@Slf4j
@Configuration
public class RabbitConfig {

//	消息变成死信 般是由于以下几种情况:
//  1、消息被拒绝 (Basic.Reject/Basic.Nack) ，井且设置 requeue 参数为 false;
//  2、消息过期;
//  3、令队列达到最大长度。

	public static final String NORMAL_EXCHANGE = "exchange.report.normal";
	public static final String NORMAL_QUEUE = "queue.report.normal";
	public static final String DLX_QUEUE = "queue.report.dlx";
	public static final String DLX_EXCHANGE = "queue.report.dlx";
	public static final String LOG_QUEUE = "queue.report.log";
	public static final String DLX_ROUTINGKEY = "routingkey.report.dlx";
	public static final String NORMAL_ROUTINGKEY = "routingkey.report.normal";
	public static final String LOG_ROUTINGKEY = "routingkey.report.log";


	/**
	 * 设置交换机类型
	 */
	@Bean
	public DirectExchange dlxExchange() {
		/**
		 * DirectExchange:按照routingkey分发到指定队列
		 * TopicExchange:多关键字匹配
		 * FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
		 * HeadersExchange ：通过添加属性key-value匹配
		 */
		return new DirectExchange(DLX_EXCHANGE, true, false);
	}

	@Bean
	public DirectExchange normalExchange() {
		/**
		 * DirectExchange:按照routingkey分发到指定队列
		 * TopicExchange:多关键字匹配
		 * FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
		 * HeadersExchange ：通过添加属性key-value匹配
		 */
		return new DirectExchange(NORMAL_EXCHANGE, true, false);
	}

	@Bean
	public Queue normalQueue() {
		Map<String, Object> args = Maps.newHashMap();
//		args.put("x-message-ttl", 3600000); //1小时
		args.put("x-message-ttl", 60000); //1小时
		args.put("x-dead-letter-exchange" , DLX_EXCHANGE);
		args.put("x-dead-letter-routing-key" , DLX_ROUTINGKEY);
		return new Queue(NORMAL_QUEUE, true, false, false, args);
	}

	@Bean
	public Queue dlxQueue() {
		return new Queue(DLX_QUEUE, true, false, false, null);
	}

	@Bean
	public Queue logQueue() {
		return new Queue(LOG_QUEUE, true, false, false, null);
	}

	@Bean
	public Binding dlxBinding() {
		return BindingBuilder.bind(dlxQueue()).to(dlxExchange()).with(DLX_ROUTINGKEY);
	}

	@Bean
	public Binding normalBinding() {
		return BindingBuilder.bind(normalQueue()).to(normalExchange()).with(NORMAL_ROUTINGKEY);
	}

	@Bean
	public Binding logBinding() {
		return BindingBuilder.bind(logQueue()).to(normalExchange()).with(LOG_ROUTINGKEY);
	}


	@Bean
	public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
		rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
			@Override
			public void confirm(CorrelationData correlationData, boolean ack, String cause) {
				log.info("消息唯一标识: " + correlationData.getId());
				log.info("确认结果：" + ack);
				log.info("失败原因：" + cause);
			}
		});
		return rabbitTemplate;
	}
}
