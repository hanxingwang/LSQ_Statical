package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

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
		String sparql1_0Path = "/home/hanxingwang/Data/SearchResult/SparqlFilterSpecial";
//		String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		FileWriter fileWriter1 = null;
		FileWriter fileWriter2 = null;
		BufferedReader bufferedReader = null;
		BufferedWriter sparql1_1Writer = null;
		BufferedWriter sparql1_0Writer = null;
		
		int sparql1_1Count = 0;
		int sparql1_0Count = 0;
		int begin, end;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			
			fileWriter1 = new FileWriter(sparql1_1Path);
			sparql1_1Writer = new BufferedWriter(fileWriter1);
			
			fileWriter2 = new FileWriter(sparql1_0Path);
			sparql1_0Writer = new BufferedWriter(fileWriter2);
			
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
							if(!isSparql1_0FilterSpecial(query)) {
								sparql1_0Count ++;
								WelldesignedUtil.isWelldesign(sparqlQuery, false);
							} else {
								sparql1_0Writer.write(sparqlQuery + "\n");
							}							
						} else {
							sparql1_1Count ++;
							
							sparql1_1Writer.write(sparqlQuery + "\n");
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
		} finally {
			try {
				sparql1_1Writer.flush();
				sparql1_1Writer.close();
				sparql1_0Writer.flush();
				sparql1_0Writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
	
	private boolean isSparql1_0FilterSpecial(Query query) throws Exception {		
		ArrayList<String> features = FragmentUtil.getFragments(query);

		if (features.contains("Regex") || features.contains("LangMatches") || features.contains("IsURI")
				|| features.contains("IsLiteral") || features.contains("IsBNode") || features.contains("Lang"))
			return true;

		return false;
	}

}
