package cn.tju.edu.util;

import java.util.ArrayList;

import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Datatype;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.If;
import org.openrdf.query.algebra.In;
import org.openrdf.query.algebra.IsBNode;
import org.openrdf.query.algebra.IsLiteral;
import org.openrdf.query.algebra.IsURI;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Lang;
import org.openrdf.query.algebra.LangMatches;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.Str;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class AndFeatureExctor extends QueryModelVisitorBase<RuntimeException> {
	private ArrayList<String> features = null;

	public AndFeatureExctor(ArrayList<String> features) {
		this.features = features;
	}

	public void visit(TupleExpr node) {
		node.visit(this);
	}

	public void meet(Join node) throws RuntimeException {
		if (!this.features.contains("And"))
			this.features.add("And");

		meetBinaryTupleOperator(node);
	}

	public void meet(Exists node) throws RuntimeException {
		if (!this.features.contains("Exists"))
			this.features.add("Exists");

		meetSubQueryValueOperator(node);
	}

	public void meet(If node) throws RuntimeException {
		if (!this.features.contains("If"))
			this.features.add("If");

		meetNode(node);
	}

	public void meet(In node) throws RuntimeException {
		if (!this.features.contains("In"))
			this.features.add("In");

		meetCompareSubQueryValueOperator(node);
	}

	public void meet(Regex node) throws RuntimeException {
		if (!this.features.contains("Regex"))
			this.features.add("Regex");

		meetBinaryValueOperator(node);
	}

	public void meet(Lang node) throws RuntimeException {
		if (!this.features.contains("Lang"))
			this.features.add("Lang");

		meetUnaryValueOperator(node);
	}

	public void meet(LangMatches node) throws RuntimeException {
		if (!this.features.contains("LangMatches"))
			this.features.add("LangMatches");

		meetBinaryValueOperator(node);
	}

	public void meet(IsBNode node) throws RuntimeException {
		if (!this.features.contains("IsBNode"))
			this.features.add("IsBNode");

		meetUnaryValueOperator(node);
	}

	public void meet(IsLiteral node) throws RuntimeException {
		if (!this.features.contains("IsLiteral"))
			this.features.add("IsLiteral");

		meetUnaryValueOperator(node);
	}

	public void meet(IsURI node) throws RuntimeException {
		if (!features.contains("IsURI"))
			this.features.add("IsURI");

		meetUnaryValueOperator(node);
	}

	public void meet(Str node) throws RuntimeException {
		if (!features.contains("Str"))
			this.features.add("Str");
		
		meetUnaryValueOperator(node);
	}

	public void meet(Datatype node) throws RuntimeException {
		if (!features.contains("Datatype"))
			this.features.add("Datatype");
		
		meetUnaryValueOperator(node);
	}
	
	public void meet(Compare node) throws RuntimeException {
		if (!node.getOperator().equals(CompareOp.EQ) && !node.getOperator().equals(CompareOp.NE))
			if (!features.contains("BiggerOrLess"))
				this.features.add("BiggerOrLess");

			meetBinaryValueOperator(node);
	}
}
