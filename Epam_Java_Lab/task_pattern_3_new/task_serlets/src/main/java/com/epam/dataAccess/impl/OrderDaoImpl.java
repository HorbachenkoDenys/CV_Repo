package com.epam.dataAccess.impl;

import com.epam.dataAccess.OrderDao;
import com.epam.model.Customer;
import com.epam.model.Order;
import com.epam.model.impl.CheesePizza;
import com.epam.model.impl.PeperoniPizza;

import java.util.SortedMap;
import java.util.TreeMap;

public class OrderDaoImpl implements OrderDao {
    private SortedMap<Long, Order> orders;

    {
        orders = new TreeMap<>() {
            {
                Order order = new Order(new Customer("Den", "1", "067-237-8015"),
                        new PeperoniPizza());
                put(order.getId(), order);
                order = new Order(new Customer("Nastya", "2", "093-688-77-13"),
                        new CheesePizza());
                put(order.getId(),order);
                order = new Order(new Customer("Petro", "3", "068-133-66-12"),
                        new CheesePizza());
                put(order.getId(),order);
                order = new Order(new Customer("Igor", "4", "063-678-77-31"),
                        new CheesePizza());
                put(order.getId(),order);
            }
        };
    }
    @Override
    public void save(Order order) {
        orders.put(order.getId(), order);
    }

    @Override
    public void update(long id, Order order) {
        orders.put(id, order);
    }

    @Override
    public void delete(long id) {
        orders.remove(id);
    }

    @Override
    public SortedMap<Long, Order> getOrders() {
        return orders;
    }

    @Override
    public Order findById(long id) {
        return orders.get(id);
    }
}
