package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.junit.Test;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import cn.tju.edu.dataUtil.FragmentUtil;

public class SatisfiabilityTest {
	@Test
	public void a_testFragment() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/ZeroResultQuery";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		BufferedWriter bw = null;
		
		
		int begin, end;
		int count = 0;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			
			String sparqlQuery = null;
			String features = null;
						
			FileWriter fw = new FileWriter("/home/hanxingwang/Data/SearchResult/ZeroResultWithFilterOrMinus");
			
			bw = new BufferedWriter(fw);

			while ((sparqlString = bufferedReader.readLine()) != null) {				
				begin = sparqlString.indexOf('\"');
				// begin = -1;
				end = sparqlString.lastIndexOf('\"');
				// end = sparqlString.length();
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

					if(features.contains("F") || features.contains("M") || features.contains("G") || features.contains("V"))
						bw.write(sparqlQuery + "\n");
					
					count ++;
					
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
				bw.flush();
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println(count);
	}
}
