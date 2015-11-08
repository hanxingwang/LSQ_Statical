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

import cn.tju.edu.dataUtil.FragmentUtil;
import cn.tju.edu.dataUtil.WelldesignedUtil;

public class SatisfiabilityTest {
	@Test
	public void a_testFragment() {
		String sparqlString = null;
//		String filePath = "/home/hanxingwang/Data/SearchResult/ZeroResultQuery";
		String filePath = "/home/hanxingwang/Data/SearchResult/ZeroResultWithFilterOrMinus";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		BufferedWriter bw = null;
		
		String sparql1_1Path = "/home/hanxingwang/Data/SearchResult/Sparql1_1Zero";
		String sparql1_0Path = "/home/hanxingwang/Data/SearchResult/SparqlFilterSpecialZero";
		String rest = "/home/hanxingwang/Data/SearchResult/RestZero";
		
		FileWriter fileWriter1 = null;
		FileWriter fileWriter2 = null;
		FileWriter restFile = null;
		
		BufferedWriter sparql1_1Writer = null;
		BufferedWriter sparql1_0Writer = null;
		BufferedWriter restBufferedWriter = null;
		
		int sparql1_1Count = 0;
		int sparql1_0Count = 0;
		int begin, end;
//		int count = 0;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			
			fileWriter1 = new FileWriter(sparql1_1Path);
			sparql1_1Writer = new BufferedWriter(fileWriter1);
			
			fileWriter2 = new FileWriter(sparql1_0Path);
			sparql1_0Writer = new BufferedWriter(fileWriter2);
			
			restFile = new FileWriter(rest);
			restBufferedWriter = new BufferedWriter(restFile);
			
			String sparqlQuery = null;
//			String features = null;
			
//			FileWriter fw = new FileWriter("/home/hanxingwang/Data/SearchResult/ZeroResultWithFilterOrMinus");
//			
//			bw = new BufferedWriter(fw);

			while ((sparqlString = bufferedReader.readLine()) != null) {				
//				begin = sparqlString.indexOf('\"');
				 begin = -1;
//				end = sparqlString.lastIndexOf('\"');
				 end = sparqlString.length();
				if (begin < end) {
					sparqlQuery = sparqlString.substring(begin + 1, end);
					
					Query query = null;
					
					try {
						query = QueryFactory.create(sparqlQuery);
					} catch (Exception e) {
						// TODO: handle exception
						continue;
					}
					
					query.getGraphURIs().clear();
//					
//					features = FragmentUtil.analysisFragment(query);
//					
////					if(query.hasGroupBy())
////						features += "G";
////					
////					if(query.hasValues())
////						features += "V";
//
//					if(features.contains("F") || features.contains("M") || features.contains("G") || features.contains("V"))
//						bw.write(sparqlQuery + "\n");
//					
//					count ++;
//					
//					if(count % 10000 == 0)
//						System.out.println("We have " + count + " sparql queries");
					
					if(!isSparql1_1(query)) {
						if(!isSparql1_0FilterSpecial(query)) {								
							restBufferedWriter.write(sparqlQuery + "\n");
						} else {
							sparql1_0Count ++;
							sparql1_0Writer.write(sparqlQuery + "\n");
						}					
					} else {
						sparql1_1Count ++;
						
						sparql1_1Writer.write(sparqlQuery + "\n");
					}
				}

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				bw.flush();
				bw.close();
				
				sparql1_0Writer.flush();
				sparql1_0Writer.close();
				sparql1_1Writer.flush();
				sparql1_0Writer.close();
				
				restBufferedWriter.flush();
				restBufferedWriter.close();
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

		if (features.contains("Regex") || features.contains("Lang") || features.contains("LangMatches")
				|| features.contains("Str") || features.contains("Datatype") || features.contains("IsURI")
				|| features.contains("IsLiteral") || features.contains("IsBNode") || features.contains("BiggerOrLess"))
			return true;

		return false;
	}
}
