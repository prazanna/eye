grammar Events;

options {
  language = Java;
}

@header {
  package com.prasanna.eye.query.parser;
  import com.prasanna.eye.query.model.QueryModel;
  import com.prasanna.eye.query.model.QueryPredicateModel;
  import com.prasanna.eye.query.model.PredicateProperty;
  import com.prasanna.eye.query.model.PredicateValue;
}

@lexer::header {
  package com.prasanna.eye.query.parser;
}

parse returns [QueryModel m] 
  : 
    ID 
    { java.util.List<QueryPredicateModel> temp = new java.util.ArrayList<QueryPredicateModel>(); }
    (
    predicate { temp.add($predicate.m); }
    )+ 
    EOF { $m = new QueryModel($ID.text, temp); }
  ;
	    
predicate returns [QueryPredicateModel m]
	:	'.' FILTER '(' ( property ',' )? value ')' { $m = new QueryPredicateModel($FILTER.text, $property.m, $value.m); }
	;

property returns [PredicateProperty m]
  : pathProperty { $m = $pathProperty.m; }
  | STRING_LITERAL { $m = new PredicateProperty<java.lang.String>($STRING_LITERAL.text, PredicateProperty.Type.TEXT); }
  ;
   
value returns [PredicateValue m]
  : STRING_LITERAL { $m = new PredicateValue<java.lang.String>($STRING_LITERAL.text, PredicateValue.Type.TEXT); }
  | regexProperty { $m = $regexProperty.m; }
  | stringLiteralArray { $m = $stringLiteralArray.m; }
  ;
     
pathProperty returns [PredicateProperty m]
  : '/' STRING_LITERAL { $m = new PredicateProperty<String>($STRING_LITERAL.text, PredicateProperty.Type.PATH); }
  ;
  
regexProperty returns [PredicateValue m]
  : '#' STRING_LITERAL { $m = new PredicateValue<java.util.regex.Pattern>(java.util.regex.Pattern.compile($STRING_LITERAL.text), PredicateValue.Type.REGEX); }
  ;
  
stringLiteralArray returns [PredicateValue m]
  : 
    '['
    { java.util.Collection<String> temp = new java.util.ArrayList<String>(); } 
    op1 = STRING_LITERAL { temp.add($op1.text); } 
    (',' 
    op2 = STRING_LITERAL { temp.add($op2.text); }
    )* 
    { $m = new PredicateValue<java.util.Collection>(temp, PredicateValue.Type.TEXT_ARRAY); }
    ']'
  ;
  
FILTER  
  :   'within'
  |   'last'
  |   're'
  |   'eq'
  |   'lt'
  |   'gt'
  |   'le'
  |   'ge'
  |   'ne'
  |   'in'
  ;
    	
ID  
  :	('a'..'z'|'A'..'Z'|'_' '-') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
  ;
    
STRING_LITERAL
	:	'"'
		{ StringBuilder b = new StringBuilder(); }
		(c=~('"'|'\r'|'\n'){ b.appendCodePoint(c);})*
		'"'
		{ setText(b.toString()); }
	;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;

