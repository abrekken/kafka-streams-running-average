package com.brekken.kafka.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ResultEvent {
    private String id;
    private double averageSpeed;
    private double averagePrice;

    public double getAveragePrice() {
        return BigDecimal.valueOf(averagePrice).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    public void setAveragePrice(double averagePrice) {
        this.averagePrice = averagePrice;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getAverageSpeed() {
        return BigDecimal.valueOf(averageSpeed).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }
}
