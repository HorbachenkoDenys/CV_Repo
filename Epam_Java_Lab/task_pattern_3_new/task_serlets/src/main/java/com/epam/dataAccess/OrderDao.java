package com.epam.dataAccess;

import com.epam.model.Order;

import java.util.SortedMap;

public interface OrderDao {
    void save(Order order);

    void update(long id, Order order);

    void delete(long id);

    SortedMap<Long, Order> getOrders();

    Order findById(long id);
}
