package com.urv.storlet.lambdastreams;

import java.util.stream.Stream;

public class LambdaExampleStorlet extends LambdaStreamsStorlet {

	@Override
	protected Stream<String> writeYourLambdas(Stream<String> stream) {
		return stream.map(s -> s);
	}
}
