package com.gemantic.utils;

import lombok.extern.slf4j.Slf4j;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

/**
 * createed By xiaoqiang
 * 2019/12/13 11:15
 */
@Slf4j
public class DataEncryptUtil {
	public static final String des_key = "report_indicator_ids";

	public static String encryptData(String data) {
		try {
			SecureRandom random = new SecureRandom();
			DESKeySpec keySpec = new DESKeySpec(des_key.getBytes());
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("des");
			SecretKey secretKey = keyFactory.generateSecret(keySpec);
			Cipher cipher = Cipher.getInstance("des");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey, random);
			byte[] cipherData = cipher.doFinal(data.getBytes());
			return new BASE64Encoder().encode(cipherData);
		}catch (Exception e){
			log.error("id加密错误---{}", e);
		}
		return data;
	}


	public static String decodeData(String data){
		try {
			byte[] decodeBuffer = new BASE64Decoder().decodeBuffer(data);
			SecureRandom random = new SecureRandom();
			DESKeySpec keySpec = new DESKeySpec(des_key.getBytes());
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("des");
			SecretKey secretKey = keyFactory.generateSecret(keySpec);
			Cipher cipher = Cipher.getInstance("des");
			cipher.init(Cipher.DECRYPT_MODE, secretKey, random);
			byte[] cipherData = cipher.doFinal(decodeBuffer);
			return new String(cipherData);
		} catch (Exception e){
			log.error("解密数据错误---{}", e);
		}
		return data;
	}

}
