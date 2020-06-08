package com.epam.model;

public class OrderRequest {
    private Order order;

    private String pizzaClass;

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public String getPizzaClass() {
        return pizzaClass;
    }

    public void setPizzaClass(String pizzaClass) {
        this.pizzaClass = pizzaClass;
    }
}
