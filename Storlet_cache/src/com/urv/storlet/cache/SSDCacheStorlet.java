package com.urv.storlet.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.storlet.common.IStorlet;
import com.ibm.storlet.common.StorletException;
import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

/**
 * 
 * @author Raul Gracia
 *
 */

public class SSDCacheStorlet implements IStorlet {
	
	//TODO: Be aware that the Docker must have access to the SSD device pointed here!
	private static final String SSD_PATH = "/cache/";
	
	private static final long CACHE_SIZE = 100*1024*1024;
	
	private static final String CACHE_EVICTION_POLICY = SimpleCache.LFU; //"LFU" or "LRU"
	private SimpleCache cacheIndex = null;
	
	public SSDCacheStorlet() {
		this.cacheIndex = new SimpleCache(CACHE_SIZE, CACHE_EVICTION_POLICY);
	}

	@Override
	public void invoke(ArrayList<StorletInputStream> inputStreams,
			ArrayList<StorletOutputStream> outputStreams,
			Map<String, String> parameters, StorletLogger log)
			throws StorletException {
		
		log.emitLog("SSDCacheStorlet Invoked");
		
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
		try{
			//TODO: Check that these parameters are correctly taken
			String request = parameters.get("requestType");
			String objectId = parameters.get("objectId");
			
			/**
			 * Write-through cache: A PUT object request is always stored
			 * at the SSD to ensure that the object copy is updated.
			 * 
			 */
			if (request.equals("PUT")){
				long fileSize = 0;
				File cachedFile = new File(SSD_PATH + objectId);
				FileOutputStream cachedFileOutputStream = new FileOutputStream(cachedFile);				
				byte[] buffer = new byte[1024*64];
				int len = 0;
				while((len = is.read(buffer)) != -1) {
					outputStream.write(buffer, 0, buffer.length);
					cachedFileOutputStream.write(buffer, 0, buffer.length);
					fileSize+= len;
				}				
				cachedFileOutputStream.close();
				//Update cache statistics and get a potential file to evict
				List<String> toEvictList = this.cacheIndex.accessCache("PUT", objectId, fileSize);
				if (toEvictList!= null){
					//Delete as many files as necessary to meet the space limit of the cache
					for (String toEvict: toEvictList){
						File toEvictFile = new File(SSD_PATH + toEvict);
						toEvictFile.delete();
					}
				}
			}else if (request.equals("GET")){
				List<String> cachedFileId = this.cacheIndex.accessCache("GET", objectId, 0);
				if (cachedFileId!=null){
					File cachedFile = new File(SSD_PATH + cachedFileId.get(0));
					is = new FileInputStream(cachedFile);
				}
				byte[] buffer = new byte[1024*64];
				int len = 0;
				while((len = is.read(buffer)) != -1) {
					outputStream.write(buffer, 0, buffer.length);
				}								
			}
			//System.out.println("---------------------------------");
			//System.out.println(cacheIndex.toString());
			//System.out.println("---------------------------------");
			
		} catch (IOException e) {
			log.emitLog("Cahing - raised IOException: " + e.getMessage());
		} finally {
			try {
				is.close();
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		log.emitLog("SSDCacheStorlet Invocation done");
	}
}
