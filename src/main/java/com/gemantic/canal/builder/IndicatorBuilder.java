package com.gemantic.canal.builder;

import com.gemantic.canal.exception.ConvertException;
import com.gemantic.report.constant.DataSource;
import com.gemantic.report.constant.DataType;
import com.gemantic.report.model.NReportDataDict;
import com.gemantic.report.model.NReportLevelTree;
import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.DictPrefix;
import com.gemantic.warehouse.model.DictTable;
import com.gemantic.warehouse.model.SchemaColumns;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static com.gemantic.report.constant.DataType.getDataType;

/**
 * createed By xiaoqiang
 * 2020/10/20 17:25
 */
@Slf4j
public class IndicatorBuilder {

	private static final String key_suffix = "key";
	private static final String date_suffix = "date";
	private static final String NULL = "null";
	//时间对比
	private static final String time_compare = "1";
	//分类对比
	private static final String classify_compare = "5";
	/*父级节点信息*/
	private NReportLevelTree nReportLevelTree;
	/*父级节点元数据信息*/
	private DictTable tableMess;
	/*dict_column元数据对应的mysql元数据信息*/
	private SchemaColumns schemaColumns;
	/*dict_column元数据对应的mysql元数据信息*/
	private DictColumn dictColumn;
	/*dict_column元数据对应的mysql元数据信息*/
	private Map<String, DictPrefix> entityMap;


	public IndicatorBuilder nReportLevelTree(NReportLevelTree nReportLevelTree) {
		this.nReportLevelTree = nReportLevelTree;
		return this;
	}

	public IndicatorBuilder tableMess(DictTable tableMess) {
		this.tableMess = tableMess;
		return this;
	}

	public IndicatorBuilder schemaColumns(SchemaColumns schemaColumns) {
		this.schemaColumns = schemaColumns;
		return this;
	}

	public IndicatorBuilder entityMap(Map<String, DictPrefix> entityMap) {
		this.entityMap = entityMap;
		return this;
	}

	public IndicatorBuilder dictColumn(DictColumn dictColumn) {
		this.dictColumn = dictColumn;
		return this;
	}



	public NReportDataDict build() throws ConvertException {
		try {
			//表在库中的id信息
			Long levelCode = nReportLevelTree.getId();
			//表名
			String tableName = nReportLevelTree.getAliasName();
			//获取表所属的维度
			String indexSort = getIndexSort();
			//获取表所属的维度
			String entityFlag = getEntityFlag();
			//业务唯一键
			String pkColumn = tableMess.getPkColumn();
			if (StringUtils.isEmpty(pkColumn)) return null;
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
			String field = dictColumn.getColumnName();
			Long defaultCompare = null;
			String dataType = null;
			if (null != schemaColumns) {
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
				defautDateFre = null;
				dynamic = null;
				compare = null;
				defautImage = null;
			}

			String enumType = getEnumType();
			Integer valueType = getDataType(dictColumn);
			long timeMillis = System.currentTimeMillis();
			String whereColumn = null;
			String name = dictColumn.getColumnComment();
			String unit = dictColumn.getColumnUnit();
			if (NULL.equals(unit)) {
				unit = null;
			}
			Long isUnitChange = null;
			if (StringUtils.isNotEmpty(unit)) {
				isUnitChange = 0L;
			}
			String isEntity = dictColumn.getIsEntity();
			NReportDataDict dataDict = initIndicator(name, timeMillis, field, "dw." + tableName, dynamic, unit,
					indexSort, levelCode, isUnitChange, valueType, defaultCompare, compare, isEntity, indexFlag,
					dataType, enumType, dateField, entityFlag, whereColumn, defautDateFre, defautImage, dictColumn.getId());
			return  dataDict;
		}catch (Exception e){
			throw new ConvertException("dict_column --> nreport_data_dict 转换错误", e);
		}
	}

	private NReportDataDict initIndicator(String name, Long timeMillis, String field, String table,
	                                      String dynamic, String unit, String indexSort,
	                                      Long levelCode, Long isUnitChange, Integer valueType, Long defaultCompare,
	                                      String compare, String isEntity, Long indexFlag, String dataType, String enumType,
	                                      String sortField, String entityFlag, String whereColumn, String defautDateFre, String defautImage, Long columnId){
		return NReportDataDict.builder()
				.dynamicField(dynamic).valueType(valueType).unit(unit).indicatorField(field).indicatorName(name).indicatorTable(table).isEntity(StringUtils.isEmpty(isEntity) ? null : Integer.valueOf(isEntity))
				.indexSort(Long.valueOf(indexSort)).levelCode(levelCode).isUnitChange(isUnitChange).defaultCompareCode(defaultCompare).entityFlag(entityFlag)
				.dispField(null).compareCode(compare).isValide(1L).imageCode(defautImage).whereColumn(whereColumn).dataRate(defautDateFre)
				.dbType("1").serviceCode("datacenter_pub").sortDirection("DESC").sortField(sortField).indexFlag(indexFlag)
				.sortValue(100).sourceName(DataSource.DATA_WAREHOUSE).specialHandlingFlag(null).tradeDateFlag(null).majorDispFlag(null)
				.numberUnit(null).updateAt(timeMillis).createAt(timeMillis).urlFlag(null).dataType(dataType).enumType(enumType).dictColumnId(columnId).build();
	}

	public static String getDynamic(String unique){
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


	private String getIndexSort() {
		String aliasName = nReportLevelTree.getAliasName();
		DictPrefix dictPrefix = entityMap.get(aliasName.split("_")[0]);
		return dictPrefix.getEntityId();
	}

	private String getEntityFlag() {
		String aliasName = nReportLevelTree.getAliasName();
		return aliasName.split("_")[0];
	}

	private String getEnumType(){
		String isDict = dictColumn.getIsDict();
		if(DataType.ISDICT.equals(isDict)){
			return dictColumn.getDictType();
		}
		return null;
	}
}
