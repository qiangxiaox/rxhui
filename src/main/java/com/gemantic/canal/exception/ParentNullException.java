package com.gemantic.canal.exception;

/**
 * 树节空异常
 * createed By xiaoqiang
 * 2020/11/11 10:17
 */
public class ParentNullException extends Exception{
	private MsgType msgType;



	public MsgType getMsgType() {
		return msgType;
	}

	public ParentNullException() {
		super();
	}

	public ParentNullException(String message, MsgType msgType) {
		super(message);
		this.msgType = msgType;
	}

	public ParentNullException(String message, Throwable cause, MsgType msgType) {
		super(message, cause);
		this.msgType = msgType;
	}

	public ParentNullException(Throwable cause, MsgType msgType) {
		super(cause);
		this.msgType = msgType;
	}

	public enum MsgType {
		dict_table,
		nreport_level_tree,
	}
}
