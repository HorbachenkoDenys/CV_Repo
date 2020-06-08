package com.epam.model.impl;

import com.epam.model.Pizza;

public class CheesePizza implements Pizza {

    private String name = "Cheese";

    private double price = 200;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public double getPrice() {
        return price;
    }
}
