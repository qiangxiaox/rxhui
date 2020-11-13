package com.gemantic.report.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gemantic.report.model.TransferDataSnapshoot;
import com.gemantic.report.repository.CheckPointRepository;
import com.gemantic.report.repository.MetadataThemeRepository;
import com.gemantic.report.repository.impl.MetadataThemeOkHttp;
import com.gemantic.report.service.TransferMission;
import com.gemantic.report.service.TransferService;
import com.gemantic.semantic.model.CheckPoint;
import com.gemantic.springcloud.model.Response;
import com.gemantic.springcloud.utils.MsgUtil;
import com.gemantic.warehouse.client.DictColumnClient;
import com.gemantic.warehouse.client.DictTableClient;
import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.DictTable;
import com.gemantic.warehouse.model.SchemaColumns;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * createed By xiaoqiang
 * 2019/12/7 13:46
 */
@Slf4j
@Service
public class TransferServiceImpl implements TransferService {

	private static final Long init_time = 0L;
	@Resource
	MetadataThemeRepository metadataThemeRepository;
	@Resource
	CheckPointRepository pointRepository;
	@Resource
	private MetadataThemeOkHttp metadataThemeOkHttp;

	private Long tableTime = init_time;

	private Long columnTime = init_time;

	@Resource
	TransferMission transferMission;

	private TransferDataSnapshoot snapshoot;

	private TransferDataSnapshoot initDataSnapshoot(Long columnStart, Long tableStart) {
		try {
			ResponseEntity<Response<List<SchemaColumns>>> schemaResponseEntity = metadataThemeRepository.selectSchemaColumns("dw");
			List<SchemaColumns> schemaColumns = schemaResponseEntity.getBody().getData();
			int cp = 1; int ps = Integer.MAX_VALUE;

			String dictColumnMess = metadataThemeOkHttp.getDictMess("/struct/dictColumn", columnStart);
			//String dictTableMess = metadataThemeOkHttp.getDictMess("/struct/dictTable", tableTime);
			ResponseEntity<Response<List<DictTable>>> allTableRes = metadataThemeRepository.selectAllTable();
			List<DictTable> allTableMess = allTableRes.getBody().getData();
			//保存字典表中最大的更新时间
			List<DictColumn> dictColumns = getJSONArrayData(dictColumnMess, DictColumn.class);
//			List<DictTable> dictTables = getJSONArrayData(dictTableMess, DictTable.class);
			List<DictTable> dictTables = allTableMess.stream()
					.filter(obj -> obj.getCreateAt() != null)
					.filter(obj -> obj.getCreateAt().compareTo(tableStart) >= 0)
					.collect(Collectors.toList());
			int nullCount = 0;
			if(CollectionUtils.isEmpty(dictColumns)){
				nullCount ++;
			}else {
				columnTime = dictColumns.stream().mapToLong(DictColumn::getCreateAt).max().getAsLong();
			}
			if(CollectionUtils.isEmpty(dictTables)) {
				nullCount ++;
			}else {
				tableTime = dictTables.stream().mapToLong(DictTable::getCreateAt).max().getAsLong();
			}
			if(nullCount == 2) return null;

			return new TransferDataSnapshoot(dictTables, dictColumns, schemaColumns, allTableMess);
		}catch (Exception e){
			log.error("初始化数据错误--{}", e);
		}
		return null;
	}

	@Override
//	@Scheduled(cron = "0 0 0 * * ? ") //0 0 0 * * ?
	public void transferToReport() throws Exception {
		long timeMillis = System.currentTimeMillis();
		//初始化查询时间
		String tablePointName = DictTable.class.getName();
		String columnName = DictColumn.class.getName();
		CheckPoint tablePoint = getCheckPoint(tablePointName);
		CheckPoint columnPoint = getCheckPoint(columnName);
		if(tablePoint != null){
			tableTime  = tablePoint.getCheckPoint() + 1;
		}else {
			tablePoint = new CheckPoint();
			tablePoint.setName(tablePointName);
			tablePoint.setCreateAt(timeMillis);
			tablePoint.setUpdateAt(timeMillis);
			tablePoint.setCheckPoint(init_time);
		}
		if(columnPoint != null){
			columnTime  = columnPoint.getCheckPoint() + 1;
		}else {
			columnPoint = new CheckPoint();
			columnPoint.setName(columnName);
			columnPoint.setCreateAt(timeMillis);
			columnPoint.setUpdateAt(timeMillis);
			columnPoint.setCheckPoint(init_time);
		}
		//初始化数据库信息
		snapshoot = this.initDataSnapshoot(columnTime, tableTime);
		if(snapshoot == null){
			return;
		}

		try {
			//处理树转换任务
			transferMission.transer(snapshoot);
		} catch (Exception e ){
			log.error("树节点转换错误-->{}", e);
		}

		//保存库中的最大时间
		if(columnTime - 1 != columnPoint.getCheckPoint()){
			columnPoint.setCheckPoint(columnTime);
			pointRepository.saveCheckPoint(columnPoint);
			log.info("checkpoint中columnTime={}", columnTime);
			columnTime = init_time;
		}
		if(tableTime - 1 != tablePoint.getCheckPoint()){
			tablePoint.setCheckPoint(tableTime);
			pointRepository.saveCheckPoint(tablePoint);
			log.info("checkpoint中tableTime={}", tableTime);
			tableTime = init_time;
		}
	}

	@Override
	public void transferPointTime(Long columnStart, Long tableStart) {
		//初始化数据库信息
		snapshoot = this.initDataSnapshoot(columnStart + 1L, tableStart + 1L);
		if(snapshoot == null){
			return;
		}
		try {
			//处理树转换任务
			transferMission.transer(snapshoot);
		} catch (Exception e ){
			log.error("树节点转换错误-->{}", e);
		}
	}

	private CheckPoint getCheckPoint(String name) throws Exception {
		CheckPoint checkPoint = null;
		if(StringUtils.isBlank(name)){
			return checkPoint;
		}
		try {
			ResponseEntity<Response<CheckPoint>> responseEntity = pointRepository.getCheckPoint(name);
			if(MsgUtil.isValidResponseMessage(responseEntity)){
				checkPoint = responseEntity.getBody().getData();
			}
		} catch (Exception e) {
			log.error("error->",e);
			throw new Exception();
		}
		return checkPoint;
	}

	private <T> List<T> getJSONArrayData(String mess, Class<T> clazz){
		JSONObject jsonObject = JSON.parseObject(mess);
		JSONObject data = jsonObject.getJSONObject("data");
		String listResult = data.getString("list");
		List<T> tList = JSONArray.parseArray(listResult, clazz);
		return tList;
	}

}
