package com.example.flinkdemo.model;

public class UserEvent {
    public String userId;
    public String action;
    public double amount;

    public UserEvent() {}

    public UserEvent(String userId, String action, double amount) {
        this.userId = userId;
        this.action = action;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "UserEvent{userId='" + userId + "', action='" + action + "', amount=" + amount + '}';
    }
}
