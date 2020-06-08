package com.epam.model.order;

import com.epam.model.OrderRequest;
import com.epam.model.Pizza;

import javax.persistence.criteria.Order;
import java.util.SortedMap;

public interface OrderService {
    void save(Order order);

    void update(long id, OrderRequest orderRequest);

    void delete(long id);

    SortedMap<Long, Order> findAll();

    Pizza getPizzaImplementation(String orderedPizza);
}
