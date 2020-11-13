package com.gemantic.report.model;

import com.gemantic.report.constant.DataSource;
import com.gemantic.report.constant.DataType;
import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.DictPrefix;
import com.gemantic.warehouse.model.DictTable;
import com.gemantic.warehouse.model.SchemaColumns;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gemantic.report.constant.DataType.getDataType;

/**
 * createed By xiaoqiang
 * 2019/12/11 10:23
 */
@Slf4j
public class IndicatorTransfer {

	private static final String key_suffix = "key";
	private static final String date_suffix = "date";
	private static final String NULL = "null";
	//时间对比
	private static final String time_compare = "1";
	//分类对比
	private static final String classify_compare = "5";
	//字段名
	private static final String index_sort = "indexSort";
	//字段名
	private static final String entity_flag = "entityType";

	private static final String where_column = "statement_type = '408001000'";
	private static final String org_financial_prefix = "org_fin";
	private static final String statement_type = "statement_type";

	private TransferDataSnapshoot transferDataSnapshoot;

	private ThemeTransfer themeTransfer;

	@Getter
	private List<NReportDataDict> reportDataDicts;

	public IndicatorTransfer(TransferDataSnapshoot transferDataSnapshoot, ThemeTransfer themeTransfer, Map entityMap) {
		this.transferDataSnapshoot = transferDataSnapshoot;
		this.themeTransfer = themeTransfer;
		reportDataDicts = Lists.newArrayList();
		initIndicator(entityMap);
	}

	private void initIndicator(Map<String, DictPrefix> entityMap){
		long timeMillis = System.currentTimeMillis();
		Map<String, DictTable> dictTable = transferDataSnapshoot.getTableMap();
		//入库的表信息
		List<NReportLevelTree> nodeInfos = themeTransfer.getTabNodeInfos();

		//需要处理的字段信息
		List<CompositeColumn> dictColumn = transferDataSnapshoot.getCompositeColumns();

		//已有的表可能加字段，所以挨个字段处理
		for(CompositeColumn column : dictColumn){
			if(column.getTableName().startsWith(DataSource.filter_prefix)) continue;
			try {
				//字段所在表
				NReportLevelTree reportLevelTree = nodeInfos.stream()
						.filter(obj -> column.getTableName().equals(obj.getAliasName()))
						.findFirst().get();
				//表在库中的id信息
				Long levelCode = reportLevelTree.getId();
				//表名
				String tableName = reportLevelTree.getAliasName();
				//表的基础信息
				DictTable tableMess = dictTable.get(tableName);
				//获取表所属的维度
				String indexSort = getIndexSort(reportLevelTree, entityMap, index_sort);
				//获取表所属的维度
				String entityFlag = getIndexSort(reportLevelTree, entityMap, entity_flag);
				//业务唯一键
				String pkColumn = tableMess.getPkColumn();
				if (StringUtils.isEmpty(pkColumn)) continue;
				//确定动态字段
				String dynamic = getDynamic(pkColumn);
				//确定对比类型
				String compare = time_compare;
				//确定查询时间字段
				String dateField = null;
				String[] dynamicField = dynamic.split(",");
				if (StringUtils.isEmpty(dynamicField[0])) {
					compare = classify_compare; //分类对比
				} else {
					dateField = dynamicField[0];
				}
				String whereColumn = null;
				if(tableName.startsWith(org_financial_prefix)){
					List<String> tableColumns = dictColumn.stream().filter(obj -> obj.getTableName().equals(tableName)).map(CompositeColumn::getColumnName).collect(Collectors.toList());
					if (tableColumns.contains(statement_type)) {
						whereColumn = where_column;
					}
				}
				DictColumn innColumn = column.getDictColumn();
				String name = innColumn.getColumnComment();
				String field = innColumn.getColumnName();
				String unit = innColumn.getColumnUnit();
				if (NULL.equals(unit)) {
					unit = null;
				}
				String isEntity = innColumn.getIsEntity();
				Long isUnitChange = null;
				if (StringUtils.isNotEmpty(unit)) {
					isUnitChange = 0L;
				}
				Long defaultCompare = null;
				//获取valueType
				Integer valueType = getDataType(column);
				SchemaColumns schemaColumns = column.getSchemaColumns();
				String dataType = null;
				if(null != schemaColumns){
					dataType = schemaColumns.getDataType();
				}
				String defautDateFre = "日,周,月,季,年,年（3月）,年（6月）,年（9月）";
				String defautImage = "1";
				Long indexFlag = null;
				if (field.equals(dynamicField[1])) {
					indexFlag = 1L; //字段标记
				}
				if (field.equals(dateField)) {
					defaultCompare = 1L; //默认对比属性设置
					indexFlag = 3L; //字段标记
					defautDateFre  = null;
					dynamic = null;
					compare = null;
					defautImage = null;
				}
				String enumType = getEnumType(column);
				NReportDataDict dataDict = initIndicator(name, timeMillis, field, "dw." + tableName, dynamic, unit,
						indexSort, levelCode, isUnitChange, valueType, defaultCompare, compare, isEntity, indexFlag,
						dataType, enumType, dateField, entityFlag, whereColumn, defautDateFre, defautImage);
				reportDataDicts.add(dataDict);
			}catch (Exception e){
				log.warn("字段=【{}】,字段名=【{}】, 所在表=【{}】,转换错误", column.getColumnName(),column.getDictColumn().getColumnComment(), column.getTableName());
				log.error("转换错误-->{}", e);
			}
		}
	}

	private NReportDataDict initIndicator(String name, Long timeMillis, String field, String table,
	                                      String dynamic, String unit, String indexSort,
	                                      Long levelCode, Long isUnitChange, Integer valueType, Long defaultCompare,
	                                      String compare, String isEntity, Long indexFlag, String dataType, String enumType,
	                                      String sortField, String entityFlag, String whereColumn, String defautDateFre, String defautImage){
		 return NReportDataDict.builder()
				 .dynamicField(dynamic).valueType(valueType).unit(unit).indicatorField(field).indicatorName(name).indicatorTable(table).isEntity(Integer.valueOf(isEntity))
				 .indexSort(Long.valueOf(indexSort)).levelCode(levelCode).isUnitChange(isUnitChange).defaultCompareCode(defaultCompare).entityFlag(entityFlag)
				 .dispField(null).compareCode(compare).isValide(1L).imageCode(defautImage).whereColumn(whereColumn).dataRate(defautDateFre)
				 .dbType("1").serviceCode("datacenter_pub").sortDirection("DESC").sortField(sortField).indexFlag(indexFlag)
				 .sortValue(100).sourceName(DataSource.DATA_WAREHOUSE).specialHandlingFlag(null).tradeDateFlag(null).majorDispFlag(null)
				 .numberUnit(null).updateAt(timeMillis).createAt(timeMillis).urlFlag(null).dataType(dataType).enumType(enumType).build();
	}

	private String getDynamic(String unique){
		//字典表中的动态字段：时间,维度唯一键（没有时间需要留下占位符）
		StringBuilder dynamic = new StringBuilder();
		String[] split = unique.replaceAll("，", ",").split(",");
		for(int x = 0; x < split.length; x++){
			if(split[x].endsWith(date_suffix)){
				dynamic.append(split[x]); break;
			}
		}
		//唯一键中会存在多个日期，以第一个日期作为查询日期
		dynamic.append(",");
		for(int x = 0; x < split.length; x++){
			if(split[x].endsWith(key_suffix)){
				dynamic.append(split[x]); break;
			}
		}
		return dynamic.toString();
	}


	private String getIndexSort(NReportLevelTree reportLevelTree, Map<String, DictPrefix> entityMap, String type){
		String[] parent = reportLevelTree.getParentPath().split(",");
		String topId = parent[parent.length - 1];
		List<NReportLevelTree> infos = themeTransfer.getTopNodeInfos();
		NReportLevelTree levelTree = infos.stream().filter(obj -> obj.getId().equals(Long.valueOf(topId))).findFirst().get();
		String aliasName = levelTree.getAliasName();
		DictPrefix dictPrefix = entityMap.get(aliasName);
		if(index_sort.equals(type)){
			return dictPrefix.getEntityId();
		}else if(entity_flag.equals(type)){
			return aliasName;
		}
		return null;
	}

	private String getEnumType(CompositeColumn column){
		String isDict = column.getDictColumn().getIsDict();
		if(DataType.ISDICT.equals(isDict)){
			return column.getDictColumn().getDictType();
		}
		return null;
	}
}
