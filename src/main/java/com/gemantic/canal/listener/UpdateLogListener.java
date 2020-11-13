package com.gemantic.canal.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.canal.event.UpdateCanalEvent;
import com.gemantic.canal.exception.ConvertException;
import com.gemantic.canal.model.LogExtraEntry;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.report.model.NReportDataDict;
import com.gemantic.report.model.NreportDataLog;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.gemantic.utils.LogMsgInitUtil.initNreportLog;

/**
 * RowData 字段信息，增量数据(修改后,新增后)
 * RowData 字段信息，增量数据(修改前,删除前) *
 */
@Slf4j
@Component
public class UpdateLogListener extends CanalListener<UpdateCanalEvent> implements ApplicationContextAware {

	private ApplicationContext applicationContext;

	@Override
	public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
		if (!"nreport_data_log".equals(table)) return;
		log.info("-------> database:{},table:{},eventType: UPDATE", database, table);
		List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
		Optional<CanalEntry.Column> isValidCol = columns.stream()
				.filter(obj -> obj.hasUpdated())
				.filter(obj -> "is_valid".equals(obj.getName()))
				.findFirst();
		if(!isValidCol.isPresent()) return;
		NreportDataLog nreportDataLog = parseColumnsToObject(columns, NreportDataLog.class);
		try {
			String logExtra = nreportDataLog.getLogExtra();
			LogExtraEntry logExtraEntry = JSON.parseObject(logExtra, LogExtraEntry.class);
			CanalEntry.Entry parseEntry = CanalEntry.Entry.parseFrom(logExtraEntry.getDataSource());
			applicationContext.publishEvent(new InsertCanalEvent(parseEntry));
		}catch (Exception e){
			log.info("日志处理错误-->{}", e);
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
