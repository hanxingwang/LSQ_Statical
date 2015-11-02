package cn.tju.edu.util.test;

import cn.tju.edu.Query.QueryStorge;
import cn.tju.edu.dataprocess.Storge;
import cn.tju.edu.util.StaticalUtil;
import org.junit.Test;

public class StaticalUtilTest {
	private static Storge storge = new Storge("/home/hanxingwang/Data/SesameStorage");
	private static QueryStorge query = new QueryStorge(storge.getConnection());

	@Test
	public void test() {
		String queryString = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> SELECT (COUNT(?id) AS ?count) WHERE {  ?id lsqv:usesFeature lsqv:Filter . ?id lsqv:usesFeature lsqv:SubQuery }";
		
		query.QueryToFile(queryString, "/home/hanxingwang/Data/SearchResult/countOptional");
	}
	
//	@Test
	public void a_getIdAndFeaturePair() {
		String queryString = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> SELECT ?id ?feature WHERE { ?id lsqv:usesFeature ?feature . FILTER (?feature IN (lsqv:Optional, lsqv:Filter, lsqv:Minus, lsqv:Union, lsqv:SubQuery)) }";
//		String queryString = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> SELECT DISTINCT ?feature WHERE { ?id lsqv:triplePatterns ?triples . ?id lsqv:usesFeature ?feature . }";
		
		query.QueryToFile(queryString, "/home/hanxingwang/Data/SearchResult/IdFeaturePairs");
	}
	
//	@Test
	public void b_StaticalFeatures() {
		StaticalUtil.staticalFeature("/home/hanxingwang/Data/SearchResult/IdFeaturePairs", "/home/hanxingwang/Data/SearchResult/FeaturesStatical");
	}
	
//	@Test
	public void c_CountFeatures() {
		StaticalUtil.CountFeature("/home/hanxingwang/Data/SearchResult/FeaturesStatical", "/home/hanxingwang/Data/SearchResult/CountFeatures");
	}
	 
}
