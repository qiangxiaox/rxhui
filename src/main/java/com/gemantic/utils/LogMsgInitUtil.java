package com.gemantic.utils;

import com.alibaba.fastjson.JSON;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.model.LogExtraEntry;
import com.gemantic.report.model.NreportDataLog;

/**
 * createed By xiaoqiang
 * 2020/11/12 15:56
 */
public class LogMsgInitUtil {

	public static NreportDataLog initNreportLog(CanalEvent event, Exception exception,
	                                                 String projectName, String clazzName, String logTitle){
		NreportDataLog.NreportDataLogBuilder builder = NreportDataLog.builder();
		builder.logTitle(logTitle);
		builder.logLevel("error");
		builder.logProject(projectName);
		builder.logType(clazzName);
		builder.logMsg(JSON.toJSONString(exception));
		LogExtraEntry.LogExtraEntryBuilder entryBuilder = LogExtraEntry.builder();
		entryBuilder.dataSource(event.getByteSource()).count(event.getCount()).javaType(event.getEntry().getClass().getName());
		builder.logExtra(JSON.toJSONString(entryBuilder.build()));
		builder.isValid(1);
		builder.logDesc(exception.getMessage());
		long timeMillis = System.currentTimeMillis();
		builder.createAt(timeMillis);
		builder.updateAt(timeMillis);
		builder.tryTime(event.getCount());
		return builder.build();
	}
}
