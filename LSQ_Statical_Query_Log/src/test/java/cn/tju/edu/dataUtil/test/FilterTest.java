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
import cn.tju.edu.dataprocess.Storge;

public class FilterTest {
	private static Storge storge = new Storge("/home/hanxingwang/Data/SesameStorage");
	private static QueryStorge query = new QueryStorge(storge.getConnection());
	
//	@Test
	public void a_testGetSource() {
		String queryString = "PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp:<http://spinrdf.org/sp#> SELECT DISTINCT ?text WHERE {  ?id lsqv:usesFeature lsqv:Filter . ?id sp:text ?text }";
		
		query.QueryToFile(queryString, "/home/hanxingwang/Data/SearchResult/Filter");
	}
	
	@Test
	public void b_testWellDesign() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/Filter";
		String sparql1_1Path = "/home/hanxingwang/Data/SearchResult/Sparql1_1";
		String regex = "/home/hanxingwang/Data/SearchResult/Regex";
		String lang = "/home/hanxingwang/Data/SearchResult/Lang";
		String is = "/home/hanxingwang/Data/SearchResult/Is";
		String rest = "/home/hanxingwang/Data/SearchResult/Rest";
		String biggerOrLess = "/home/hanxingwang/Data/SearchResult/BiggerOrLess";
//		String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		FileWriter fileWriter1 = null;
		FileWriter fileWriter2 = null;
		FileWriter fileWriter3 = null;
		FileWriter fileWriter4 = null;
		FileWriter fileWriter5 = null;
		FileWriter fileWriter6 = null;
		
		BufferedReader bufferedReader = null;
		BufferedWriter sparql1_1Writer = null;
		BufferedWriter regexBufferedWriter = null;
		BufferedWriter langBufferedWriter = null;
		BufferedWriter isBufferedWriter = null;
		BufferedWriter restBufferedWriter = null;
		BufferedWriter biggerOrLessBufferedWriter = null;
		
		int sparql1_1Count = 0;
		int sparql1_0Count = 0;
		int begin, end;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			
			fileWriter1 = new FileWriter(sparql1_1Path);
			sparql1_1Writer = new BufferedWriter(fileWriter1);
			
			fileWriter2 = new FileWriter(regex);
			regexBufferedWriter = new BufferedWriter(fileWriter2);
			fileWriter3 = new FileWriter(lang);
			langBufferedWriter = new BufferedWriter(fileWriter3);
			fileWriter4 = new FileWriter(is);
			isBufferedWriter = new BufferedWriter(fileWriter4);
			fileWriter5 = new FileWriter(rest);
			restBufferedWriter = new BufferedWriter(fileWriter5);
			fileWriter6 = new FileWriter(biggerOrLess);
			biggerOrLessBufferedWriter = new BufferedWriter(fileWriter6);
			
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
					
					try {
						if(!isSparql1_1(query)) {							
							if(!isRegex(query)) {
								if(!isLangMatches(query)) {
									if(!isIs(query)) {
										if(!biggerOrLess(query))
											restBufferedWriter.write(sparqlQuery + "\n");
										else
											biggerOrLessBufferedWriter.write(sparqlQuery + "\n");
									} else {
										isBufferedWriter.write(sparqlQuery + "\n");
									}
								} else {
									langBufferedWriter.write(sparqlQuery + "\n");
								}
							} else {
								regexBufferedWriter.write(sparqlQuery + "\n");
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
				regexBufferedWriter.flush();
				regexBufferedWriter.close();
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
	
	private boolean isRegex(Query query) throws Exception {		
		ArrayList<String> features = FragmentUtil.getFragments(query);

		if (features.contains("Regex"))
			return true;

		return false;
	}
	
	private boolean isLangMatches(Query query) throws Exception {		
		ArrayList<String> features = FragmentUtil.getFragments(query);

		if (features.contains("Lang") || features.contains("LangMatches") || features.contains("Str") || features.contains("Datatype"))
			return true;

		return false;
	}
	
	private boolean isIs(Query query) throws Exception {
		ArrayList<String> features = FragmentUtil.getFragments(query);
		
		if(features.contains("IsURI")|| features.contains("IsLiteral") || features.contains("IsBNode"))
			return true;
		
		return false;
	}
	
	private boolean biggerOrLess(Query query) throws Exception {
		ArrayList<String> features = FragmentUtil.getFragments(query);
		
		if(features.contains("BiggerOrLess"))
			return true;
		
		return false;
	}
}
