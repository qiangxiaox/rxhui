package com.gemantic.canal.exception;

import com.gemantic.report.model.NReportLevelTree;
import lombok.Getter;
import lombok.Setter;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.Map;

/**
 * createed By xiaoqiang
 * 2020/11/11 15:49
 */
public class HttpErrorException extends Exception {
	@Getter
	@Setter
	private RequestMethod requestMethod;
	@Getter
	@Setter
	private String mess;
	@Getter
	@Setter
	private Class<?> clazz;

	public HttpErrorException() {
		super();
	}

	public HttpErrorException(String message) {
		super(message);
	}

	public HttpErrorException(String message, Throwable cause) {
		super(message, cause);
	}

	public HttpErrorException(Throwable cause) {
		super(cause);
	}
}
