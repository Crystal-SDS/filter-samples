package com.urv.storlet.lambdastreams;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.ibm.storlet.common.IStorlet;
import com.ibm.storlet.common.StorletException;
import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

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
	
	private static Character DELIMITER = '\n';
	
	protected Map<String, String> parameters = null;
	
	/**
	 * This method is intended to be implemented by developers aiming at
	 * executing some computations on data streams. The developer only
	 * needs to operate on the stream passed by parameter and return it,
	 * so the storlet can write the result on the output stream. 
	 * 
	 * @param Raw data stream
	 * @return Processed data stream
	 */
	protected abstract Stream<String> writeYourLambdas(Stream<String> stream);
	
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
		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		//Store this variable inside the object to exploit dynamic lambdas passed as parameters
		this.parameters = parameters;
		
		//Red InputStream as a Stream, and apply functions
		//TODO: Put more efforts on doing the read/write process and stream conversion more efficient
		BufferedWriter writeBuffer = new BufferedWriter(new OutputStreamWriter(os));		
		try (BufferedReader readBuffer = new BufferedReader(new InputStreamReader(is))) {
			writeYourLambdas(readBuffer.lines()).forEach(line -> {
			try {
				writeBuffer.write(line + DELIMITER);
			}catch(IOException e){
				logger.emitLog(this.getClass().getName() + " raised IOException: " + e.getMessage());
			}});
			writeBuffer.close();
			is.close();
			os.close();
		} catch (IOException e1) {
			logger.emitLog(this.getClass().getName() + " raised IOException: " + e1.getMessage());
		}
		
        long after = System.nanoTime();
		logger.emitLog(this.getClass().getName() + " -- Elapsed [ms]: "+((after-before)/1000000L));
		
	}
	
	protected <T> Stream<T> asStream(Iterator<T> sourceIterator) {
	    return asStream(sourceIterator, false);
	}
	
	protected <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
	    Iterable<T> iterable = () -> sourceIterator;
	    return StreamSupport.stream(iterable.spliterator(), parallel);
	}
}