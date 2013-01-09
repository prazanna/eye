package com.prasanna.eye.web.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import com.prasanna.eye.model.TimedEvent;
import com.prasanna.eye.buffer.EventBuffer;

@Controller
public class EventController {
  private Logger log = LoggerFactory.getLogger(EventController.class);

  @Autowired
  private EventBuffer<TimedEvent> timedEventBuffer;

  @RequestMapping(value = "/events", method = RequestMethod.POST)
  public @ResponseBody void digestEvent(@RequestBody TimedEvent timerEvent) {
    log.info("Received request of type " + timerEvent.getType());
    timedEventBuffer.offer(timerEvent);
  }
}
