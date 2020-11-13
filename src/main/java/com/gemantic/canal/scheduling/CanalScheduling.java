package com.gemantic.canal.scheduling;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.Message;
import com.gemantic.canal.event.DeleteCanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.canal.event.UpdateCanalEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;


@Slf4j
@Component
public class CanalScheduling implements Runnable, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Resource
    private CanalConnector canalConnector;

    @Scheduled(fixedDelay = 100)
    @Override
    public void run() {
        try {
            //获取指定数量的数据
            int batchSize = 1000;
            Message message = canalConnector.getWithoutAck(batchSize);
            // 数据批号
            long batchId = message.getId();
            log.debug("scheduled_batchId=" + batchId);
            try {
                List<Entry> entries = message.getEntries();
                if (batchId != -1 && entries.size() > 0) {
                    entries.forEach(entry -> {
                        if (entry.getEntryType() == EntryType.ROWDATA) {
                            //事件发布
                            publishCanalEvent(entry);
                        }
                    });
                }
                canalConnector.ack(batchId);// 提交确认
            } catch (Exception e) {
                log.info("发送监听事件失败！batchId回滚,batchId=" + batchId, e);
                //这次回滚后下次激活会继续收到这个binlog推送
                canalConnector.rollback(batchId);
            }
        } catch (Exception e) {
            log.error("canal_scheduled异常！", e);
        }
    }

    public void publishCanalEvent(Entry entry) {
        EventType eventType = entry.getHeader().getEventType();
        switch (eventType) {
            case INSERT:
                applicationContext.publishEvent(new InsertCanalEvent(entry));
                break;
            case UPDATE:
                applicationContext.publishEvent(new UpdateCanalEvent(entry));
                break;
            case DELETE:
                applicationContext.publishEvent(new DeleteCanalEvent(entry));
                break;
            default:
                break;
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
