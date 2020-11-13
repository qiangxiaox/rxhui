package com.gemantic.report.controller;

import com.gemantic.report.constant.DictPrefixConstant;
import com.gemantic.springcloud.model.Response;
import com.gemantic.springcloud.model.Responses;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * createed By xiaoqiang
 * 2019/12/7 16:06
 */
@Slf4j
@Api(value = "更新常量")
@RestController
@RequestMapping(value = "/constant")
public class ConstantController {
	@Resource
	private DictPrefixConstant dictPrefixConstant;

	@ApiOperation(value = "更新词典前缀", notes = "更新词典前缀")
	@GetMapping("/dictPrefix")
	public ResponseEntity<Response<Void>> updateDictPrefix() {
		log.info("更新词典前缀");
		dictPrefixConstant.updateData();
		return Responses.ok();
	}
}
