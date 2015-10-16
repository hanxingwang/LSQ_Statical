package cn.tju.edu.util.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.tju.edu.util.StringUtil;

public class LSQLocalUtilTest {
//	private LSQLocalUtil lsq = new LSQLocalUtil();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
//		lsq.CountTriplePattern("/home/hxw/DataPath");
		StringUtil.replace("", "^.*#", "/home/hxw/Data/SearchResult/QueryWithFeatures", "/home/hxw/Data/SearchResult/QueryWithFeaturesWithoutPrefix");
		StringUtil.replace("", "id=http://lsq.aksw.org/res/", "/home/hxw/Data/SearchResult/QueryWithFeaturesWithoutPrefix", "/home/hxw/Data/SearchResult/QueryWithFeaturesMap");
	}

}
