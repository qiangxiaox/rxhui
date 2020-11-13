package com.gemantic.report.constant;

import com.gemantic.report.model.CompositeColumn;
import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.SchemaColumns;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * createed By xiaoqiang
 * 2019/12/11 17:12
 */
public class DataType {

	private static Map<Integer, List<String>> dataType = Maps.newHashMap();
	private static final Integer NUMBER = 1;
	private static final Integer STR = 2;
	private static final Integer DATE = 5;
	private static final Integer ENUM = 6;
	public static final String ISDICT = "1";
	static {
		String[] numarr = new String[]{"tinyint", "smallint", "mediumint", "int", "bigint", "float", "double", "real", "decimal"};
		List<String> number = Arrays.asList(numarr);
		dataType.put(NUMBER, number);

		String[] strarr = new String[]{"char", "varchar", "tinytext", "text", "mediumtext", "longtext"};
		List<String> str = Arrays.asList(strarr);
		dataType.put(STR, str);

		String[] datearr = new String[]{"date", "time", "datetime", "timestamp", "year"};
		List<String> date = Arrays.asList(datearr);
		dataType.put(DATE, date);
	}


	public static Integer getDataType(CompositeColumn column){
		try {
			String isDict = column.getDictColumn().getIsDict();
			String type = column.getSchemaColumns().getDataType();
			if(ISDICT.equals(isDict)){
				return ENUM; //枚举值
			}else {
				return dataType.entrySet().stream()
						.filter(obj -> obj.getValue().contains(type))
						.map(obj -> obj.getKey()).findFirst().get();
			}
		}catch (Exception e){
			return null;
		}
	}

	public static Integer getDataType(DictColumn dictColumn){
		String isDict = dictColumn.getIsDict();
		if(ISDICT.equals(isDict)) return ENUM; //枚举值

		String unit = dictColumn.getColumnUnit();
		if(StringUtils.isNotEmpty(unit)) return NUMBER;

		String columnName = dictColumn.getColumnName();
		if(columnName.endsWith("date")) return DATE;

		return STR;
	}

}
