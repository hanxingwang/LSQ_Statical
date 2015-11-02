package cn.tju.edu.dataUtil.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class SeasameTest {
	@Test
	public void a_testSeasame() {
		String sparqlString = null;
		String filePath = "/home/hanxingwang/Data/SearchResult/QueryText";
		// String filePath = "/home/hanxingwang/Data/SearchResult/NotUnionFree";
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;

		ArrayList<String> tupleExprNames = null;

		int begin, end;
		int count = 0;
		try {
			fileReader = new FileReader(filePath);
			bufferedReader = new BufferedReader(fileReader);
			String sparqlQuery = null;
			String tupleExprName = null;

			Projection projection = null;
			tupleExprNames = new ArrayList<String>();

			while ((sparqlString = bufferedReader.readLine()) != null) {
				begin = sparqlString.indexOf('\"');
				// begin = -1;
				end = sparqlString.lastIndexOf('\"');
				// end = sparqlString.length();
				if (begin < end) {
					sparqlQuery = sparqlString.substring(begin + 1, end);

					Query query = new Query();
					try {
						query = QueryFactory.create(sparqlQuery);
					} catch (Exception e) {
						continue;
					}

					query.getGraphURIs().clear();
					sparqlQuery = query.toString();

					SPARQLParser parser = new SPARQLParser();
					ParsedQuery parsedQuery = null;
					TupleExpr tupleExpr = null;
					try {
						parsedQuery = parser.parseQuery(sparqlQuery, null);
						tupleExpr = parsedQuery.getTupleExpr();
						
						while (!Projection.class.isInstance(tupleExpr)) {
							UnaryTupleOperator unaryTupleOperator = null;
							unaryTupleOperator = (UnaryTupleOperator)tupleExpr;
							tupleExpr = unaryTupleOperator.getArg();
						}
						
						projection = (Projection)tupleExpr;
						
						tupleExprName = projection.getArg().getClass().getSimpleName();

						if (!tupleExprNames.contains(tupleExprName))
							tupleExprNames.add(tupleExprName);

						count++;
						if (count % 10000 == 0) {
							System.out.println("We have " + count + " sparql queries");
						}
					} catch (MalformedQueryException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
		
		System.out.println(count);

		for (String tupleExprName : tupleExprNames) {
			System.out.println(tupleExprName);
		}

	}
}
