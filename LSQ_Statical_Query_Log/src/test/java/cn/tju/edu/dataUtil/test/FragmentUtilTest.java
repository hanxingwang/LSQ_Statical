package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import cn.tju.edu.dataUtil.FragmentUtil;

public class FragmentUtilTest {	
	@Test
	public void a_testFragment() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/RestZero";
		String filePath2 = "/home/hanxingwang/Data/SearchResult/OZero";
		// String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		
		FileWriter fileWriter = null;
		BufferedWriter bufferedWriter = null;
		
		Map<String, Integer> featureCouples = new HashMap<String, Integer>();
		
		int begin, end;
		int count = 0;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			
			fileWriter = new FileWriter(filePath2);
			bufferedWriter = new BufferedWriter(fileWriter);
			
			String sparqlQuery = null;
			String features = null;
			
			Integer oldCount, newCount;

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
					
					features = FragmentUtil.analysisFragment(query);
					
					if(query.hasGroupBy())
						features += "G";
					
					if(query.hasValues())
						features += "V";

					if(features == null || features.trim().equals(""))
						System.err.println("nononono");
						
					count ++;
					
					if(features.contains("O")) {
						bufferedWriter.write(sparqlString + "\n");
					}
					
					if(featureCouples.containsKey(features)) {
						oldCount = featureCouples.get(features);
						newCount = new Integer(oldCount + 1);
						featureCouples.put(features, newCount);
					} else {
						featureCouples.put(features, new Integer(1));
					}
					
					if(count % 10000 == 0)
						System.out.println("We have " + count + " sparql queries");
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
				bufferedWriter.flush();
				bufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		System.out.println(count);
		
		Set<String> featuresCoupleSet = featureCouples.keySet();
		for(String feature : featuresCoupleSet) {
			System.out.println(feature + "\t" + featureCouples.get(feature));
		}
	}
}
