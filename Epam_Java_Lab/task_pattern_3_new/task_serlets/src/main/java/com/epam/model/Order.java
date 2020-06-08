package com.epam.model;

import java.util.concurrent.atomic.AtomicLong;

public class Order {
    private static AtomicLong count = new AtomicLong(0);

    private long id;

    private Customer customer;

    private Pizza orderedPizza;

    public Order() {}

    public  Order (Customer customer, Pizza orderedPizza) {
        this.customer = customer;
        this.orderedPizza = orderedPizza;
        id = count.incrementAndGet();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Pizza getOrderedPizza() {
        return orderedPizza;
    }

    public void setOrderedPizza(Pizza orderedPizza) {
        this.orderedPizza = orderedPizza;
    }
}
