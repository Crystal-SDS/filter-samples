package com.urv.storlet.lambdapushdown;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.openstack.storlet.common.IStorlet;
import org.openstack.storlet.common.StorletException;
import org.openstack.storlet.common.StorletInputStream;
import org.openstack.storlet.common.StorletLogger;
import org.openstack.storlet.common.StorletObjectOutputStream;
import org.openstack.storlet.common.StorletOutputStream;

/**
 * 
 * This Storlet is intended to execute Java 8 stream functions (map, filter)
 * onto the byte-level streams of Storlets. This will probably open new opportunities
 * for easily program Storlets with the existing Streams framework.
 * 
 * @author Raul Gracia
 *
 */

public abstract class LambdaStreamsStorlet implements IStorlet {
	
	protected final Charset CHARSET = Charset.forName("UTF-8");
	protected final int BUFFER_SIZE = 64*1024;
	
	protected Map<String, String> parameters = null;
	
	/**
	 * This method is intended to be implemented by developers aiming at
	 * executing some computations on textual data streams. The developer only
	 * needs to operate on the stream passed by parameter and return it,
	 * so the storlet can write the result on the output stream. The signature
	 * of the method is tied to Strings, as we primary focus on textual data
	 * processing (e.g., logs, CSV,...). Working with other types of objects 
	 * in a stream should be done by the developer by implementing the necessary 
	 * transformations within the method. 
	 * 
	 * @param Raw data stream
	 * @return Processed data stream
	 */	
	@SuppressWarnings("rawtypes")
	protected abstract Stream writeYourLambdas(Stream<String> stream);
	
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams, Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
		
		long before = System.nanoTime();
		logger.emitLog("----- Init " + this.getClass().getName() + " -----");
		
		//Get streams and parameters
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
		StorletObjectOutputStream sos = (StorletObjectOutputStream) outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		//TODO: Performance problem here: We are dealing with characters, not bytes, so we use
		//BufferedWriter/Reader. This is convenient for executing lambdas, but we get worse
		//performance compared to managing input/output streams in bytes.
		applyLambdasOnDataStream(is, os, logger);
		
        long after = System.nanoTime();
		logger.emitLog(this.getClass().getName() + " -- Elapsed [ms]: "+((after-before)/1000000L));		
	}	
	
	@SuppressWarnings("unchecked")
	protected void applyLambdasOnDataStream(InputStream is, OutputStream os, StorletLogger logger) {
		try{
			//Convert InputStream as a Stream, and apply lambdas
			BufferedWriter writeBuffer = new BufferedWriter(new OutputStreamWriter(os, CHARSET), BUFFER_SIZE);
			BufferedReader readBuffer = new BufferedReader(new InputStreamReader(is, CHARSET), BUFFER_SIZE); 
			writeYourLambdas(readBuffer.lines()).forEach(line -> {	
				try {
					writeBuffer.write(line.toString());  
					writeBuffer.newLine();
				}catch(IOException e){
					logger.emitLog(this.getClass().getName() + " raised IOException: " + e.getMessage());
					e.printStackTrace(System.err);
				}
			});
			writeBuffer.close();
			is.close();
			os.close();
		} catch (IOException e1) {
			logger.emitLog(this.getClass().getName() + " raised IOException 2: " + e1.getMessage());
			e1.printStackTrace(System.err);
		}		
	}
}