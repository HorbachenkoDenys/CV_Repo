package com.epam.model.order;

import com.epam.dataAccess.OrderDao;
import com.epam.enums.PizzaTypes;
import com.epam.model.Customer;
import com.epam.model.OrderRequest;
import com.epam.model.Pizza;

import javax.persistence.criteria.Order;
import java.util.SortedMap;

public class OrderServiceImpl implements OrderService {
    private OrderDao orderDao;

    public OrderServiceImpl(OrderDao orderDao) {
        this.orderDao = orderDao;
    }

    @Override
    public void save(Order order) {
        orderDao.save(order);
    }

    @Override
        public void update(long id, OrderRequest orderRequest) {
            Order order = orderDao.findOneById(id);
            order.setId(id);
            Order requestOrder = orderRequest.getOrder();
            Customer requestCustomer = requestOrder.getCustomer();
            String pizzaImplementation = orderRequest.getPizzaClass();
            Pizza pizza;
        if (pizzaImplementation !=null) {
            pizza = getPizzaImplementation(pizzaImplementation);
            order.setOrderPizza(pizza);
        }
        if (requestCustomer != null) {
            if (requestCustomer.getName() != null) {
                order.getCustomer().setFirstNAme(requestCustomer.getName());
            }
            if (requestCustomer.getId() != null) {
                order.getCustomer().setId(requestCustomer.getId());
            }
            if (requestCustomer.getPhoneNumber() != null) {
                order.getCustomer().setPhoneNumber(requestCustomer.getPhoneNumber());
            }
        }
        orderDao.update(id, order);
    }

    @Override
    public void delete(long id) {
        orderDao.delete(id);
    }

    @Override
    public SortedMap<Long, Order> findAll() {
        return orderDao.getOrders();
    }

    @Override
    public Pizza getPizzaImplementation(String orderedPizza) {
        Pizza pizza = null;
        if (orderedPizza.equalsIgnoreCase(PizzaTypes.PEPPERONI.toString())) {
            pizza = new PepperoniPizza();
        }
        else if (orderedPizza.equalsIgnoreCase(PizzaTypes.CHEESE.toString())) {
            pizza = new CheesePizza();
        }
        else if (orderedPizza.equalsIgnoreCase(PizzaTypes.CARBONARA.toString())) {
            pizza = new CarnonaraPizza();
        }
        else if (orderedPizza.equalsIgnoreCase(PizzaTypes.VEGAN.toString())) {
            pizza = new VeganPizza();
        }
        return pizza;
    }
}
