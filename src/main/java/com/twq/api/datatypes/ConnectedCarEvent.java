package com.twq.api.datatypes;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

public class ConnectedCarEvent implements Comparable<ConnectedCarEvent> {
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")
                    .withLocale(Locale.US).withZoneUTC();

    // 表示每一次乘车的唯一标识
    private String id;

    // 表示每一辆车的唯一标识
    private String carId;

    // 表示事件产生的时间戳
    private long timestamp;

    // 表示车所在位置的经度
    private float longitude;

    // 表示车所在位置的纬度
    private float latitude;

    // 表示耗油量
    private float consumption;

    // 表示车速
    private float speed;

    // 表示油门的位置(%)
    private float throttle;

    // 表示发动机负荷(%)
    private float engineload;

    public String getId() {
        return id;
    }

    public String getCarId() {
        return carId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public float getLongitude() {
        return longitude;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getConsumption() {
        return consumption;
    }

    public float getSpeed() {
        return speed;
    }

    public float getThrottle() {
        return throttle;
    }

    public float getEngineload() {
        return engineload;
    }

    public static ConnectedCarEvent fromString(String line) {
        String[] tokens = line.split("(,|;)\\s*");
        if (tokens.length != 23) {
            throw new RuntimeException("Invalid record:" + line);
        }

        ConnectedCarEvent event = new ConnectedCarEvent();

        try {
            event.id = tokens[1];
            event.carId = tokens[0];
            event.timestamp = DateTime.parse(tokens[22], timeFormatter).getMillis();
            event.longitude = tokens[20].length() > 0 ? Float.parseFloat(tokens[20]) : 0.0f;
            event.latitude = tokens[21].length() > 0 ? Float.parseFloat(tokens[21]) : 0.0f;
            event.consumption = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            event.speed = tokens[9].length() > 0 ? Float.parseFloat(tokens[9]) : 0.0f;
            event.throttle = tokens[12].length() > 0 ? Float.parseFloat(tokens[12]) : 0.0f;
            event.engineload = tokens[19].length() > 0 ? Float.parseFloat(tokens[19]) : 0.0f;
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid field: " + line, nfe);
        }

        return event;
    }

    @Override
    public String toString() {
        return "ConnectedCarEvent{" +
                "id='" + id + "\'," +
                " carId='" + carId + "\'," +
                " timestamp=" + timestamp +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", consumption=" + consumption +
                ", speed=" + speed +
                ", throttle=" + throttle +
                ", engineload=" + engineload + '}';
    }

    @Override
    public int compareTo(ConnectedCarEvent o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }
}
