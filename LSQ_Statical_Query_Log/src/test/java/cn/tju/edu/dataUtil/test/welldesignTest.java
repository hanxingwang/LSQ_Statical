package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import cn.tju.edu.Query.QueryStorge;
import cn.tju.edu.dataUtil.Storge;
import cn.tju.edu.dataUtil.WelldesignUtil;

public class welldesignTest {
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
//		String filePath = "/home/hanxingwang/Data/subQuery";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		
		int begin, end;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			
			while ((sparqlString = bufferedReader.readLine()) != null) {
				begin = sparqlString.indexOf('\"');
//				begin = -1;
				end = sparqlString.lastIndexOf('\"');
//				end = sparqlString.length();
				if(begin < end)
					WelldesignUtil.isWelldesign(sparqlString.substring(begin+1, end), false);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
