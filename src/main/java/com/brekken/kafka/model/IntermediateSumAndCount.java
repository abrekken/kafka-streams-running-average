package com.brekken.kafka.model;

public class IntermediateSumAndCount {
    private long count;
    private double speedSum;
    private double priceSum;
    private double averageSpeed;
    private double averagePrice;

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public double getAveragePrice() {
        return averagePrice;
    }

    public void setAveragePrice(double averagePrice) {
        this.averagePrice = averagePrice;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSpeedSum() {
        return speedSum;
    }

    public void setSpeedSum(double speedSum) {
        this.speedSum = speedSum;
    }

    public double getPriceSum() {
        return priceSum;
    }

    public void setPriceSum(double priceSum) {
        this.priceSum = priceSum;
    }
}
