package com.urv.storlet.lambdastreams.examples;

import java.util.stream.Stream;

import com.urv.storlet.lambdastreams.LambdaStreamsStorlet;

/**
 * Simple noop storlet to test the efficiency of Java 8 Streams.
 * 
 * @author Raul Gracia
 *
 */
public class LambdaNoopStorlet extends LambdaStreamsStorlet {

	@Override
	protected Stream<String> writeYourLambdas(Stream<String> stream) {
		return stream.map(s -> s);
	}
	
}