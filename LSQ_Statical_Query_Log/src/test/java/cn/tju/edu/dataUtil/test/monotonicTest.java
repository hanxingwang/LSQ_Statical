package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import cn.tju.edu.dataUtil.MonotonicUtil;

public class monotonicTest {
	@Test
	public void a_testMonotonic() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/QueryText";
		// String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;

		int begin, end;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			String sparqlQuery = null;

			while ((sparqlString = bufferedReader.readLine()) != null) {
				begin = sparqlString.indexOf('\"');
				// begin = -1;
				end = sparqlString.lastIndexOf('\"');
				// end = sparqlString.length();
				if (begin < end) {
					sparqlQuery = sparqlString.substring(begin + 1, end);

					if (isSparql1_1(sparqlQuery))
						MonotonicUtil.isMonotonicity(sparqlQuery, false);
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
		Matcher graphMatcher = graphPattern.matcher(upper);

		if (graphMatcher.find())
			return false;
		else
			return true;
	}
}
