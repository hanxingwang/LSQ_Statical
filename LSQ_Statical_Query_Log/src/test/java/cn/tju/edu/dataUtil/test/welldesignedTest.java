package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import cn.tju.edu.Query.QueryStorge;
import cn.tju.edu.dataUtil.FragmentUtil;
import cn.tju.edu.dataUtil.WelldesignedUtil;
import cn.tju.edu.dataprocess.Storge;

public class welldesignedTest {
	private static Storge storge = new Storge("/home/hanxingwang/Data/SesameStorage");
	private static QueryStorge query = new QueryStorge(storge.getConnection());
	
//	@Test
	public void a_testGetSource() {
		String queryString = "PREFIX sp:<http://spinrdf.org/sp#> SELECT DISTINCT ?text WHERE {  ?id sp:text ?text }";
		
		query.QueryToFile(queryString, "/home/hanxingwang/Data/SearchResult/QueryText");
	}
	
	@Test
	public void b_testWellDesign() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/QueryText";
		String sparql1_1Path = "/home/hanxingwang/Data/SearchResult/Sparql1_1";
//		String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		
		int sparql1_1Count = 0;
		int sparql1_0Count = 0;
		int begin, end;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			String sparqlQuery = null;
			
			Query query = null;
			
			while ((sparqlString = bufferedReader.readLine()) != null) {
				begin = sparqlString.indexOf('\"');
//				begin = -1;
				end = sparqlString.lastIndexOf('\"');
//				end = sparqlString.length();
				if(begin < end) {
					sparqlQuery = sparqlString.substring(begin+1, end);
					
					try {
						query = QueryFactory.create(sparqlQuery);
					} catch (Exception e) {
						// TODO: handle exception
						continue;
					}
					
					if(sparqlQuery.contains("NOT EXISTS"))
						System.err.println();
					try {
						if(!isSparql1_1(query)) {
							sparql1_0Count ++;
							WelldesignedUtil.isWelldesign(sparqlQuery, false);
						} else {
							sparql1_1Count ++;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						continue;
					}
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Sparql 1.0 is " + sparql1_0Count + " .");
		System.out.println("Sparql 1.1 is " + sparql1_1Count + " .");
	}
	
	private boolean isSparql1_1(Query query) throws Exception {
		if(query.hasGroupBy())
			return true;
		
		if(query.hasHaving())
			return true;
		
		if(query.hasValues())
			return true;
		
		ArrayList<String> features = FragmentUtil.getFragments(query);

		if (features.contains("Minus") || features.contains("Bind") || features.contains("Graph")
				|| features.contains("Exists") || features.contains("In") || features.contains("SubQuery")
				|| features.contains("If"))
			return true;

		return false;
	}

}
