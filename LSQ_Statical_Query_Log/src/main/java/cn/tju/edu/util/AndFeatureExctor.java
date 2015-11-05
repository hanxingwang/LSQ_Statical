package cn.tju.edu.util;

import java.util.ArrayList;

import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.If;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
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
}
