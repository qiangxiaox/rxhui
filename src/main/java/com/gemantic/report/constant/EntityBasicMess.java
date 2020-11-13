package com.gemantic.report.constant;

import com.gemantic.springcloud.model.PageResponse;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.client.DictColumnClient;
import com.gemantic.warehouse.vo.DictColumn;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * createed By xiaoqiang
 * 基本信息表中的字段
 * 2019/12/21 15:48
 */
@Slf4j
@Service
public class EntityBasicMess {
	private static String[] mess = new String[]{"stk_dim_info", "org_dim_info", "regi_dim", "indu_dim_sw", "indu_dim_csrc", "fmr_dim_info", "fmrt_dim_info"};
	public static final List<String> basicMessList = new ArrayList<>(Arrays.asList(mess));

	@Getter
	private Map<String, List<String>> basicMessMap = Maps.newHashMap();

	@Resource
	private DictColumnClient dictColumnClient;

	@EventListener
	public void initConstant(ContextRefreshedEvent event){
		Map<String, String> param = Maps.newHashMap();
		param.put("tableName", StringUtils.join(mess, ","));
		try {
			Response<PageResponse<DictColumn>> responseResponse = dictColumnClient.find(null, null,
					null, null, null, null, null, null,
					null, 1, Integer.MAX_VALUE, param, null, null);
			List<DictColumn> columns = responseResponse.getData().getList();
			Map<String, List<String>> listMap = columns.stream().collect(Collectors.groupingBy(DictColumn::getTableName,
					Collectors.mapping(DictColumn::getColumnComment, Collectors.toList())));
			basicMessMap.putAll(listMap);
		} catch (Exception e) {
			log.error("获取基本信息表结构失败--{}", e);
		}
	}

}
