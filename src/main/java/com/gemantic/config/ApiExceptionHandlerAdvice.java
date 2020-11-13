package com.gemantic.config;

import com.gemantic.springcloud.model.ResponseMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolationException;
import java.util.MissingFormatArgumentException;

/**
 * 拦截 controller throw 的错误
 * @author yhye 2016年11月25日上午11:01:24
 */

@RestControllerAdvice
public class ApiExceptionHandlerAdvice {

	private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandlerAdvice.class);

	private static  final int SERVER_ERROR1=-5001;

	private static  final int SERVER_ERROR2=-5002;


	@ExceptionHandler(value = { ConstraintViolationException.class, MissingServletRequestParameterException.class,
			TypeMismatchException.class, IllegalArgumentException.class, IllegalStateException.class,MissingFormatArgumentException.class })
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	public ResponseMessage constraintViolationException(HttpServletRequest request, Exception ex) {
		LOG.error(request.getRequestURI()+"?"+request.getQueryString(),ex);
		return ResponseMessage.error(HttpStatus.BAD_REQUEST.value(), SERVER_ERROR1, ex.getMessage());
	}

	@ExceptionHandler(value = { Exception.class })
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	public ResponseMessage unknownException(HttpServletRequest request, Exception ex) {
		LOG.error(request.getRequestURI()+"?"+request.getQueryString(),ex);
		return ResponseMessage.error(HttpStatus.INTERNAL_SERVER_ERROR.value(), SERVER_ERROR2, ex.getMessage());
	}

}
