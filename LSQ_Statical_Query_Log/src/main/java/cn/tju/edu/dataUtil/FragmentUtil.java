package cn.tju.edu.dataUtil;

import java.util.ArrayList;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementWalker;

import cn.tju.edu.util.AndFeatureExctor;
import cn.tju.edu.util.FragmentExctor;

public class FragmentUtil {
	private static ArrayList<String> features = new ArrayList<String>();
	
	public static String analysisFragment(Query query) {
		String fragment = "";
		
		
		SPARQLParser parser = new SPARQLParser();
		ParsedQuery parsedQuery = null;
		TupleExpr tupleExpr = null;
		
		Element element = null;
		AndFeatureExctor andVisitor = null;
		FragmentExctor fragmentVisitor = null;
		
		try {
			parsedQuery = parser.parseQuery(query.toString(), null);
			tupleExpr = parsedQuery.getTupleExpr();
			
			features = new ArrayList<String>();
			
			andVisitor = new AndFeatureExctor(features);
			andVisitor.visit(tupleExpr);
			
			element = query.getQueryPattern();
			fragmentVisitor = new FragmentExctor(features);
			
			if(element != null) {
				ElementWalker.walk(element, fragmentVisitor);
			}
			
			if(features.contains("Optional"))
				fragment += "O";
			
			if(features.contains("Filter"))
				fragment += "F";
			
			if(features.contains("And"))
				fragment += "A";
			
			if(features.contains("Union"))
				fragment += "U";
			
			if(features.contains("Minus"))
				fragment += "M";
			
			if(features.contains("SubQuery")) 
				fragment += "S";			
			
			if(fragment.trim().equals("")) {
				while(tupleExpr instanceof UnaryTupleOperator) {
					UnaryTupleOperator unaryTupleOperator = (UnaryTupleOperator)tupleExpr;
					tupleExpr = unaryTupleOperator.getArg();
					
					if(tupleExpr instanceof SingletonSet) {
						fragment += "E";
						break;
					}
				}
				
				if(tupleExpr instanceof StatementPattern || tupleExpr instanceof ArbitraryLengthPath)
					fragment += "I";
			}
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return fragment;
	}	
}
