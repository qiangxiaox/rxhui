package com.gemantic.canal.msg.log;

import com.gemantic.config.RabbitConfig;
import com.gemantic.report.model.NreportDataLog;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Arrays;

/**
 * createed By xiaoqiang
 * 2020/11/12 11:19
 */
@Slf4j
@Component
public class LogListener {

	@Resource
	private TreeReportServiceRepository treeReportServiceRepository;

	@RabbitListener(queues = RabbitConfig.LOG_QUEUE)
	public void process(NreportDataLog nreportDataLog, Channel channel, Message message) throws IOException {
		try {
			treeReportServiceRepository.saveLog(Arrays.asList(nreportDataLog));
			// 框架容器，是否开启手动ack按照框架配置
			channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
		} catch (Exception e) {
			//丢弃这条消息
			channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
			log.info("处理消息错误，重新入队-->{}", e);
		}
	}
}
