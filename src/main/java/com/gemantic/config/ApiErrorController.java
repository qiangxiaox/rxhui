/**
 *
 */
package com.gemantic.config;

import com.gemantic.springcloud.model.ResponseMessage;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletRequest;

/**
 * 404统一返回处理
 *
 * @author yhye 2016年11月25日下午6:02:46
 */
@ApiIgnore
@RestController
public class ApiErrorController implements ErrorController {
	private static final String ERROR_PATH = "/error";
	private static final int ERROR_NOT_FOUND = -4041;

	@RequestMapping(value = ERROR_PATH)
	@ResponseStatus(HttpStatus.NOT_FOUND)
	@ResponseBody
	public ResponseMessage error(HttpServletRequest request) {
		return ResponseMessage.error(HttpStatus.NOT_FOUND.value(), ERROR_NOT_FOUND, null);
	}

	@Override
	public String getErrorPath() {
		return ERROR_PATH;
	}

}
