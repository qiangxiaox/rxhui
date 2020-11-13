package com.gemantic.report.controller;

import com.gemantic.report.constant.DictPrefixConstant;
import com.gemantic.report.service.TransferService;
import com.gemantic.springcloud.model.Response;
import com.gemantic.springcloud.model.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * createed By xiaoqiang
 * 2019/12/10 13:56
 */
@Slf4j
@Api(value = "开始导入")
@RestController
@RequestMapping(value = "/transfer")
public class TransferMissionController {

	@Resource
	private TransferService transferService;

	@ApiOperation(value = "导数据", notes = "导数据")
	@GetMapping
	public ResponseEntity<Response<Void>> updateDict() throws Exception {
		log.info("更新词典前缀");
		transferService.transferToReport();
		return Responses.ok();
	}

	@ApiOperation(value = "指定开始时间跑数据", notes = "指定开始时间跑数据")
	@GetMapping("/point/time")
	public ResponseEntity<Response<Void>> transferPointTime(
			@RequestParam("columnTime") Long columnStartTime,
			@RequestParam("tableStartTime") Long tableStartTime
	) throws Exception {
		log.info("更新词典前缀");
		transferService.transferPointTime(columnStartTime, tableStartTime);
		return Responses.ok();
	}
}
