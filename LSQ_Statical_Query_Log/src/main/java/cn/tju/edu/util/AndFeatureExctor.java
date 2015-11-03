package cn.tju.edu.util;

import java.util.ArrayList;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class AndFeatureExctor extends QueryModelVisitorBase<RuntimeException> {
	private ArrayList<String> features = null;
//	private int projectionCount = 0;
//
//	public int getProjectionCount() {
//		return projectionCount;
//	}
//
//	public void setProjectionCount(int projectionCount) {
//		this.projectionCount = projectionCount;
//	}

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

//	public void meet(LeftJoin node) throws RuntimeException {
//		if (!this.features.contains("Optional"))
//			this.features.add("Optional");
//
//		meetBinaryTupleOperator(node);
//	}
//
//	public void meet(Filter node) throws RuntimeException {
//		if (!this.features.contains("Filter"))
//			this.features.add("Filter");
//
//		meetUnaryTupleOperator(node);
//	}
//	
//	public void meet(Union node) throws RuntimeException {
//		if (!this.features.contains("Union"))
//			this.features.add("Union");
//
//		meetBinaryTupleOperator(node);
//	}
//	
//	public void meet(Difference node) throws RuntimeException {
//		if (!this.features.contains("Minus"))
//			this.features.add("Minus");
//
//		meetBinaryTupleOperator(node);
//	}
//	
//	public void meet(Projection node) throws RuntimeException {
//		projectionCount ++;
//
//		meetUnaryTupleOperator(node);
//	}
//	
//	public void meet(MultiProjection node) throws RuntimeException {
//		projectionCount ++;
//
//		meetUnaryTupleOperator(node);
//	}
}
