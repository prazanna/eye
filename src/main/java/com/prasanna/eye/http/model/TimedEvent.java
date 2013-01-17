package com.prasanna.eye.http.model;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

@XmlRootElement
public class TimedEvent implements Serializable {
  private static final long serialVersionUID = 1L;
  private static ObjectMapper objectMapper = new ObjectMapper();
  private String type;
  private Date time;
  private byte[] data;

  public TimedEvent() {

  }

  public TimedEvent(final String type, final Date time, final byte[] data) {
    this.type = type;
    this.time = time;
    this.data = data;
  }

  public String getType() {
    return type;
  }

  public Date getTime() {
    return time;
  }

  public byte[] getData() {
    return data;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public void setTime(final Date time) {
    this.time = time;
  }

  public void setData(final byte[] data) {
    this.data = data;
  }

  @Override
  public String toString() {
    return "TimedEvent{" +
        "type='" + type + '\'' +
        ", time=" + time +
        '}';
  }

  public static TimedEvent fromJson(JsonNode jsonNode) throws IOException, ParseException {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    String eventType = jsonNode.get("type").getTextValue();
    Date eventDate = format.parse(jsonNode.get("time").getTextValue());
    JsonNode dataNode = jsonNode.get("data");
    byte[] raw = objectMapper.writeValueAsBytes(dataNode);
    return new TimedEvent(eventType, eventDate, raw);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimedEvent)) {
      return false;
    }

    final TimedEvent that = (TimedEvent) o;

    if (!Arrays.equals(data, that.data)) {
      return false;
    }
    if (time != null ? !time.equals(that.time) : that.time != null) {
      return false;
    }
    if (type != null ? !type.equals(that.type) : that.type != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (time != null ? time.hashCode() : 0);
    result = 31 * result + (data != null ? Arrays.hashCode(data) : 0);
    return result;
  }
}
