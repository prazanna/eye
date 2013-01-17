package com.prasanna.eye.http.controller;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.EventQueryParser;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.storage.EventStorage;

@Controller
@RequestMapping(value = "/events")
public class EventController {
  private Logger log = LoggerFactory.getLogger(EventController.class);

  private EventQueryParser eventParser;
  private EventStorage<TimedEvent> eventStorage;

  public void setEventSearch(final EventQueryParser parser) {
    this.eventParser = parser;
  }

  public void setEventStorage(final EventStorage<TimedEvent> eventStorage) {
    this.eventStorage = eventStorage;
  }

  @RequestMapping(method = RequestMethod.POST)
  public @ResponseBody void digestEvent(@RequestBody JsonNode timedEvent) throws IOException, ParseException {
    log.info("Received timedEvent " + timedEvent);
    TimedEvent event = TimedEvent.fromJson(timedEvent);
    eventStorage.storeEvents(event);
  }

  @RequestMapping(method = RequestMethod.GET)
  public
  @ResponseBody
  List<TimedEvent> searchEvents(@RequestParam("q") String query) {
    log.info("Searching for events " + query);
    QueryModel queryModel = eventParser.parseEventQuery(query);
    return eventStorage.queryEvents(queryModel);
  }
}
