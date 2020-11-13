package com.gemantic.report.repository;

import com.alibaba.fastjson.JSONArray;
import com.gemantic.report.model.NReportDataDict;
import com.gemantic.report.model.NReportIndicatorClassify;
import com.gemantic.report.model.NReportLevelTree;
import com.gemantic.report.model.NreportDataLog;
import com.gemantic.springcloud.model.Response;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * createed By xiaoqiang
 * 2019/7/24 16:41
 */
@FeignClient(name = "treeReportServiceRepository", url = "${tree.report.service.url}")
public interface TreeReportServiceRepository {

	@RequestMapping(value = "/indicator/detail/{id}",method = RequestMethod.GET)
	ResponseEntity<Response<NReportDataDict>> indicatorDetail(@PathVariable(value = "id") Long id);

	@RequestMapping(value = "/indicator/{ids}",method = RequestMethod.GET)
	ResponseEntity<Response<List<NReportDataDict>>> getIndicators(@PathVariable(value = "ids") String ids);

	/**
	 * 查询所有顶级对比属性
     *
	 * @return
	 */
	@RequestMapping(value = "/classify", method = RequestMethod.GET)
	ResponseEntity<Response<List<NReportIndicatorClassify>>>  getAllTopNodes();

	/**
	 * 查询所有顶级对比属性
	 *
	 * @return
	 */
	@RequestMapping(value = "/classify/all", method = RequestMethod.GET)
	ResponseEntity<Response<List<NReportIndicatorClassify>>> getAllCategory();

    /**
     * 根据分类ID查询树的信息
     *
     * @param   classifyId 分类(对比属性)ID
     * @return
     */
    @RequestMapping(value = "/tree", method = RequestMethod.GET)
	ResponseEntity<Response<JSONArray>> getTreeList(@RequestParam("classifyId") String classifyId);

	@RequestMapping(value = "/query",method = RequestMethod.POST)
	ResponseEntity<Response<Map<String,List<Map<String,Object>>>>> query(@RequestBody Map<String, String> queryMapping);

	@RequestMapping(value = "/query/simple/sql", method = RequestMethod.GET)
	ResponseEntity<Response<List<Map<String, Object>>>> querySimple(@RequestParam("sql") String sql);

	@RequestMapping(value = "/tree/mapping",method = RequestMethod.GET)
	ResponseEntity<Response<Map<String, List<Long>>>> getMapping();

	@RequestMapping(value = "/tree",method = RequestMethod.POST)
	ResponseEntity<Response<List<NReportLevelTree>>> saveTree(@RequestBody List<NReportLevelTree> levelTrees);

	@RequestMapping(value = "/indicator",method = RequestMethod.POST)
	ResponseEntity<Response<List<NReportDataDict>>> saveIndicator(@RequestBody List<NReportDataDict> dataDicts);

	@RequestMapping(value = "/tree",method = RequestMethod.PUT)
	ResponseEntity<Response<Boolean>> updateValid();

	@RequestMapping(value = "/tree/new", method = RequestMethod.GET)
	ResponseEntity<Response<List<NReportLevelTree>>> getNewTreeList();

	@RequestMapping(path = "/tree/refresh",  method = RequestMethod.GET)
	ResponseEntity<Response<Void>> refresh();

	@RequestMapping(path = "/leveltree/byTable",  method = RequestMethod.DELETE)
	ResponseEntity<Response<Void>> deleteLevelTreeByDictTableId(@RequestParam("dictTableId") Long dictTableId);

	@RequestMapping(path = "/indicator/byColumnId",  method = RequestMethod.DELETE)
	ResponseEntity<Response<Void>> deleteIndicatorByColumnId(@RequestParam(value = "columnId") Long columnId);

	@RequestMapping(path = "/leveltree",  method = RequestMethod.GET)
	ResponseEntity<Response<NReportLevelTree>> findTreeByTableId(@RequestParam("dictTableId") Long dictTableId);

	@RequestMapping(path = "/leveltree/aliasName",  method = RequestMethod.GET)
	ResponseEntity<Response<List<NReportLevelTree>>> findTreeByAliasName(@RequestParam("aliasNames") List<String> aliasNames);

	@RequestMapping(path ="/indicator/levelCode",  method = RequestMethod.GET)
	ResponseEntity<Response<List<NReportDataDict>>> indicatorDetailByLevelCode(@RequestParam(value = "levelCode") Long levelCode);

	@RequestMapping(path ="/indicator/detail/columnId",  method = RequestMethod.GET)
	ResponseEntity<Response<NReportDataDict>> indicatorDetailByColumnId(@RequestParam(value = "columnId") Long columnId);

	@RequestMapping(value = "/nreport/log", method = RequestMethod.POST)
	ResponseEntity<Response<List<NreportDataLog>>> saveLog(@RequestBody List<NreportDataLog> logList);
}
