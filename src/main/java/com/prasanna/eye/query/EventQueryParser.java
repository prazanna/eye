package com.prasanna.eye.query;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.query.parser.EventsLexer;
import com.prasanna.eye.query.parser.EventsParser;

public class EventQueryParser {
  public QueryModel parseEventQuery(String query) {
    CharStream stream = new ANTLRStringStream(query);
		EventsLexer lexer = new EventsLexer(stream);
		TokenStream tokenStream = new CommonTokenStream(lexer);
		EventsParser parser = new EventsParser(tokenStream);
		try {
			return parser.parse();
		} catch (RecognitionException e) {
			throw new IllegalQueryException(query, e);
		}
  }
}
