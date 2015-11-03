package cn.tju.edu.util;

import java.util.ArrayList;

import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementMinus;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.sparql.syntax.ElementVisitorBase;
import com.hp.hpl.jena.sparql.syntax.ElementWalker;

public class FragmentExctor  extends ElementVisitorBase
{
    private ArrayList<String> features = null;
    
    public FragmentExctor(ArrayList<String> features) {
		// TODO Auto-generated constructor stub
    	this.features = features;
	}

    @Override
    public void visit(ElementUnion el) {
       if(!this.features.contains("Union"))
    	   this.features.add("Union");
    }

    @Override
    public void visit(ElementOptional el) {
        if(!this.features.contains("Optional"))
        	this.features.add("Optional");
    }
    
  
    @Override
    public void visit(ElementFilter el) {
       if(!this.features.contains("Filter"))
    	   this.features.add("Filter");
    }
    
    @Override
    public void visit(ElementMinus el) {
    	 if(!this.features.contains("Minus"))
      	   this.features.add("Minus");
    }
    
    @Override
    public void visit(ElementSubQuery el) {
    	if(!this.features.contains("SubQuery"))
       	   this.features.add("SubQuery");

        Element subEl = el.getQuery().getQueryPattern();

        ElementWalker.walk(subEl, this);
    }
}