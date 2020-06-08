package com.epam.model.impl;

import com.epam.model.Pizza;

public class PeperoniPizza implements Pizza {

    private String name = "Peperoni";

    private double price = 150;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public double getPrice() {
        return price;
    }
}
