package com.urv.storlet.crypto;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.urv.storlet.sql.IStorlet;

/**
 * 
 * @author Raul Gracia
 *
 */

public class AESEncryptionStorlet implements IStorlet {
	
	@Override
	public void invoke(ArrayList<StorletInputStream> inputStreams,
			ArrayList<StorletOutputStream> outputStreams,
			Map<String, String> parameters, StorletLogger log)
			throws StorletException {
		
		log.emitLog("CryptoStorlet Invoked");
		
		/*
		 * Prepare streams
		 */
		StorletInputStream sis = inputStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
		
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outputStreams.get(0);
		storletObjectOutputStream.setMetadata(metadata);
		OutputStream outputStream = storletObjectOutputStream.getStream();
		
		/*
		 * Initialize encryption engine
		 */
		String initVector = "RandomInitVector"; // 16 bytes IV
		IvParameterSpec iv = null;
		SecretKeySpec skeySpec = null;
		Cipher cipher = null;
		String key = "getActualKeyFromRedis"; //TODO: Get keys from Redis
		try {
			iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
			skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
	        cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");        
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidAlgorithmParameterException e) {
			e.printStackTrace();
		}

		final int ENCRYPT = 0;
		final int DECRYPT = 1;

		int action = ENCRYPT;
		
		/*
		 * Get optional action flag
		 */
        String action_str = parameters.get("action");
		if (action_str != null && action_str.equals("reverse")) {
			action = DECRYPT;
		}

		try {
			byte[] buffer = new byte[65536];
			int len;
			if (action == ENCRYPT) {
				cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);				
				while((len=is.read(buffer)) != -1) {
					byte[] encrypted = cipher.update(buffer);
					outputStream.write(encrypted, 0, encrypted.length);
				}
			} else {
				cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
				while((len = is.read(buffer)) != -1) {
					byte[] decrypted = cipher.update(buffer);
					outputStream.write(decrypted, 0, decrypted.length);
				}
			}
		} catch (IOException e) {
			log.emitLog("Encryption - raised IOException: " + e.getMessage());
		} finally {
			try {
				is.close();
				outputStream.close();
			} catch (IOException e) {
			}
		}
		log.emitLog("EncryptionStorlet Invocation done");
	}
}
