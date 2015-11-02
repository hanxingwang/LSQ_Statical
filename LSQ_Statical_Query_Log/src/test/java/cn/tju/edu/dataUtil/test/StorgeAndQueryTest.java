package cn.tju.edu.dataUtil.test;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import cn.tju.edu.Query.QueryStorge;
import cn.tju.edu.dataprocess.Storge;

@FixMethodOrder(MethodSorters.DEFAULT)
public class StorgeAndQueryTest {
	private static Storge storge = new Storge("/home/hanxingwang/Data/SesameStorage");
	private static QueryStorge query = new QueryStorge(storge.getConnection());
	
//	@Test
	public void a_testStorge() {
		storge.StorgeRDFFromFile("/home/hanxingwang/Data/LSQ/LSQ-DBpedia-Seasame.ttl");
		storge.StorgeRDFFromFile("/home/hanxingwang/Data/LSQ/LSQ-BM-Seasame.ttl");
		storge.StorgeRDFFromFile("/home/hanxingwang/Data/LSQ/LSQ-LGD-Seasame.ttl");
		storge.StorgeRDFFromFile("/home/hanxingwang/Data/LSQ/LSQ-SWDF-Seasame.ttl");
	}
	
//	@Test
	public void b_testQuery() {
		String queryString1 = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> SELECT (COUNT(?id) AS ?count) WHERE {  ?id lsqv:endpoint ?ep }";
		String queryString2 = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> SELECT (COUNT(?id) AS ?count) WHERE {  ?id lsqv:triplePatterns ?triplePatterns }";
		String queryString3 = "PREFIX lsqv:<http://lsq.aksw.org/vocab#>  PREFIX sp:<http://spinrdf.org/sp#> SELECT ?triplePatterns  WHERE {   ?id lsqv:triplePatterns ?triplePatterns }";
		
		query.QueryToFile(queryString1, "/home/hanxingwang/Data/SearchResult/TotalCount");
		query.QueryToFile(queryString2, "/home/hanxingwang/Data/SearchResult/TriplePatternsCount");
		query.QueryToFile(queryString3, "/home/hanxingwang/Data/SearchResult/TriplePatternsStatistical");
	}
	
	@Test
	public void c_testQueryFeatures() {
		String queryString = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp:<http://spinrdf.org/sp#> SELECT ?text WHERE {  ?id lsqv:usesFeature lsqv:SubQuery. ?id sp:text ?text  }";
		
		query.QueryToFile(queryString, "/home/hanxingwang/Data/SearchResult/SubQuery");
	}

}
