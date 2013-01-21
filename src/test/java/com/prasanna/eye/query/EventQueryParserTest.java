package com.prasanna.eye.query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kubek2k.springockito.annotations.ReplaceWithMock;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.storage.EventStorage;

@ContextConfiguration(loader = SpringockitoContextLoader.class,
    locations = "classpath:/spring/eye-beans.xml")
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class EventQueryParserTest extends AbstractJUnit4SpringContextTests {
  @ReplaceWithMock
  @Autowired
  private EventStorage hbaseEventStorage;
  @Autowired
  private EventQueryParser queryParser;

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testParseEventQuery() throws Exception {
    EventQueryParser queryParser = new EventQueryParser();
    QueryModel result = queryParser.parseEventQuery("request.last(\"4hours\").eq(/\"service\"," +
        "#\"Cluster\").eq(/\"service\",#\"Cluster\")");
    System.out.println(result);
  }
}
