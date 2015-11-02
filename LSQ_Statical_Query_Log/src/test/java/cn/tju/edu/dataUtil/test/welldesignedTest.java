package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import cn.tju.edu.Query.QueryStorge;
import cn.tju.edu.dataUtil.WelldesignedUtil;
import cn.tju.edu.dataprocess.Storge;

public class welldesignedTest {
	private static Storge storge = new Storge("/home/hanxingwang/Data/SesameStorage");
	private static QueryStorge query = new QueryStorge(storge.getConnection());
	
//	@Test
	public void a_testGetSource() {
		String queryString = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp:<http://spinrdf.org/sp#> SELECT ?text WHERE {  ?id lsqv:triplePatterns ?triples. ?id sp:text ?text }";
		
		query.QueryToFile(queryString, "/home/hanxingwang/Data/SearchResult/QueryText");
	}
	
	@Test
	public void b_testWellDesign() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/QueryText";
//		String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		
		int begin, end;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			String sparqlQuery = null;
			
			while ((sparqlString = bufferedReader.readLine()) != null) {
				begin = sparqlString.indexOf('\"');
//				begin = -1;
				end = sparqlString.lastIndexOf('\"');
//				end = sparqlString.length();
				if(begin < end) {
					sparqlQuery = sparqlString.substring(begin+1, end);
					
					if(isSparql1_1(sparqlQuery))
						WelldesignedUtil.isWelldesign(sparqlQuery, false);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private boolean isSparql1_1(String sparqlQuery) {
		String upper = sparqlQuery.toUpperCase();
		
		Pattern graphPattern = Pattern.compile(" *GRAPH *");
		Pattern minusPattern = Pattern.compile(" *MINUS *");
		
		Matcher graphMatcher = graphPattern.matcher(upper);
		Matcher minusMatcher = minusPattern.matcher(upper);
		
		if(graphMatcher.find() || minusMatcher.find()) 
			return false;
		else
			return true;
	}

}
