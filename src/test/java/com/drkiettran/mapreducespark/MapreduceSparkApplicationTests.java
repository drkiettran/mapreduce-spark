package com.drkiettran.mapreducespark;

import org.junit.jupiter.api.Test;

//@SpringBootTest
class MapreduceSparkApplicationTests {

	@Test
	void testRun() throws Exception {
		String[] args = { "wc", "/user/student/shakespeare/tragedy/othello.txt", "/tmp/othello" };
		MapreduceSparkApplication mrsa = new MapreduceSparkApplication();
		mrsa.run(args);
	}

}
