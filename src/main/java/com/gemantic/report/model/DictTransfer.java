package com.gemantic.report.model;

import com.gemantic.dict.model.Dict;
import com.gemantic.dict.support.DictSynonym;
import com.gemantic.report.constant.EntityBasicMess;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.gemantic.report.constant.DataSource.split_placeholder;

/**
 * createed By xiaoqiang
 * 2019/12/12 17:07
 */
public class DictTransfer {

	private List<NReportDataDict> reportDataDicts;
	@Getter
	private List<Dict> resultDict;
	//知识图谱 来源 1表示数仓
	private static final String source = "1";
	//知识图谱 词典类型 "属性"
	private static final String dict_type = "属性";

	private List<Dict> existDict = Lists.newArrayList();

	public DictTransfer(List<NReportDataDict> reportDataDicts, Map<String, List<String>> basicMess, List<Dict> propDict) {
		this.reportDataDicts = reportDataDicts;
		resultDict = Lists.newArrayList();
		if(CollectionUtils.isNotEmpty(propDict)){
			existDict.addAll(propDict);
		}
		initDict(basicMess);
	}

	private void initDict(Map<String, List<String>> basicMess) {
		long timeMillis = System.currentTimeMillis();
		//指标根据名称分组，获取重复名称对应的指标id集合
		Map<String, List<Long>> nameMap = reportDataDicts.stream()
				.collect(Collectors.groupingBy(NReportDataDict::getIndicatorName,
						Collectors.mapping(NReportDataDict::getId, Collectors.toList())));
		//基本信息表中的词
		List<String> basicDict = basicMess.entrySet().stream()
				.map(obj -> obj.getValue()).flatMap(List::stream).collect(Collectors.toList());
		//保存重复的词，过滤重复
		List<String> dictRepeat = Lists.newArrayList();
		for(NReportDataDict reportDataDict : reportDataDicts){
			String name = reportDataDict.getIndicatorName();
			String indicatorTable = reportDataDict.getIndicatorTable();
			String table = indicatorTable.substring(indicatorTable.indexOf(".") + 1);
			String dictId = reportDataDict.getId().toString();
			Optional<Dict> optionalDict = existDict.stream().filter(obj -> obj.getBaseName().equals(name)).findAny();
			if(optionalDict.isPresent()){
				dictRepeat.add(name);
				Dict dict = optionalDict.get();
				String repeatIds = dict.getDictId();
				dict.setDictId(repeatIds + split_placeholder + dictId);
				resultDict.add(dict);
				continue;
			}
			//如果重复字段，则直接过滤
			if(dictRepeat.contains(name)) continue;
			//1.分析主题，确定位置
			Long dimension = getDimension(table);
			if(null == dimension) continue;
			List<Long> dictTypeIds = Arrays.asList(dimension);
			//获取指标名称对应的指标id集合
			List<Long> dataDicts = nameMap.get(name);
			if(dataDicts.size() > 1 ){
				//冗余字段
				if(basicDict.contains(name)){
					if(EntityBasicMess.basicMessList.contains(table)){
						//维度信息表中的字段
						dictRepeat.add(name);
					}else {
						//事实表中的冗余字段
						continue;
					}
				}else {
					//重复字段
					dictRepeat.add(name);
					dictId = StringUtils.join(dataDicts, split_placeholder);
				}
			}
			Dict dict = initDict(name, dictId, timeMillis, dictTypeIds);
			resultDict.add(dict);
		}
	}

	private Dict initDict(String name, String dictId, Long timeMilles, List<Long> dictTypeIds){
		DictSynonym dictSynonym = new DictSynonym();
		dictSynonym.setName(name.toLowerCase());dictSynonym.setIsBlack(false);dictSynonym.setUpdateAt(timeMilles);
		Dict dict = new Dict();
		dict.setBaseName(name);dict.setDictId(dictId);dict.setDispName(name);
		dict.setHumanSynonyms(null);dict.setSource(source);dict.setSynonyms(Arrays.asList(dictSynonym));
		dict.setType(dict_type);dict.setUpdateAt(timeMilles);dict.setCreateAt(timeMilles);
		dict.setDictTypeIds(dictTypeIds);
		return dict;
	}


	private static final Long finance = 41L;
	private static final Long macroscopic = 43L;
	private static final Long basicInfo = 46L;
	private static final Long stock_share = 75L;
	private static final Long basic_price = 45L;


	private Long getDimension(String table){
		if("stk_dim_share_chan".equals(table)) return stock_share;
		String[] themes = table.split(split_placeholder);
		switch (themes[1]) {
			case "mar": //基础报价
				return basic_price;
			case "dim"://基本信息
				return basicInfo;
			case "fin": //财务指标
				return finance;
			case "macro": //宏观指标
				return macroscopic;
			default:
				return null;
		}
	}
}
