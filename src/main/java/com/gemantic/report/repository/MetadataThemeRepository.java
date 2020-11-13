package com.gemantic.report.repository;

import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.*;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * @author Yezhiwei
 * 2019/11/27
 */
@FeignClient(name = "metadataThemeRepository", url = "${metadata.theme.service.url}")
public interface MetadataThemeRepository {

	@RequestMapping(value = "/dictColumn/all", method = RequestMethod.GET)
	ResponseEntity<Response<List<DictColumn>>> selectAllColumn();

	@RequestMapping(value = "dictPrefix", method = RequestMethod.GET)
	ResponseEntity<Response<List<DictPrefix>>> selectPrefix();

	@RequestMapping(value = "/dictTable/all", method = RequestMethod.GET)
	ResponseEntity<Response<List<DictTable>>> selectAllTable();

	@RequestMapping(value = "/schemaColumns/schema", method = RequestMethod.GET)
	ResponseEntity<Response<List<SchemaColumns>>> selectSchemaColumns(@RequestParam("schemaName") String schemaName);

	@RequestMapping(value = "/schemaStatistics/businessIndex", method = RequestMethod.GET)
	ResponseEntity<Response<List<SchemaStatistics>>> selectBusinessIndex(
			@RequestParam("schemaName") String schemaName,
			@RequestParam("indexPrefix") String indexPrefix
	);

	@RequestMapping(value = "/schemaTables/schema", method = RequestMethod.GET)
	ResponseEntity<Response<List<SchemaTables>>> selectSchemaTables(@RequestParam("schemaName") String schemaName);

	@RequestMapping(value = "/schemaColumns/stc", method = RequestMethod.GET)
	ResponseEntity<Response<SchemaColumns>> selectSchemaColumn(
			 @RequestParam String schemaName,
			 @RequestParam String tableName,
			 @RequestParam String columnName
	);

	@RequestMapping(value = "/dictTable/tableName", method = RequestMethod.GET)
	ResponseEntity<Response<DictTable>> selectTableByTableName(@RequestParam("tableName") String tableName);

}
