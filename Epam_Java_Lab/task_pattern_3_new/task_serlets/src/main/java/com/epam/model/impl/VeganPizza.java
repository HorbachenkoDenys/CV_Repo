package com.epam.model.impl;

import com.epam.model.Pizza;

public class VeganPizza implements Pizza {

    private String name = "Vegan";

    private int price = 250;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public double getPrice() {
        return price;
    }
}
