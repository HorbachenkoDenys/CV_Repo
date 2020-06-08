package com.epam.controller;

import com.epam.dataAccess.impl.OrderDaoImpl;
import com.epam.model.Customer;
import com.epam.model.Pizza;
import com.epam.model.order.OrderService;
import com.epam.model.order.OrderServiceImpl;
import com.google.gson.Gson;

import javax.persistence.criteria.Order;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

@WebServlet("/orders/*")
public class CreateNewOrder extends HttpServlet {
    private OrderService orderService;

    private Gson gson;

    @Override
    public void init() throws ServletException {
        orderService = new OrderServiceImpl(new OrderDaoImpl());
        gson = new Gson();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html;charset=UTF-8");
        req.setAttribute("orders", orderService.findAll());
        req.getRequestDispatcher("orders.jsp").forward(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String orderPizza = req.getParameter("pizza");

        Pizza pizza = orderPizza.getPizzaImpl(orderPizza);
        Order order = new Order(
                new Customer(req.getParameter("Name"), req.getParameter("id"), req.getParameter("phoneNumber")),
                );
        orderService.save(order);
        resp.sendRedirect("/orders");
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String path = req.getPathInfo();

        String[] strings = path.split("/");
        String id = splits[1];

        StringBuilder builder = new StringBuilder();
        BufferedReader reader = req.getReader();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
        }
        long orderId = Long.parseLong(id);
        OrderRequest orderRequest = gson.fromJson(builder.toString(), OrderRequest.class);
        orderService.update(orderId, orderRequest);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String path = req.getPathInfo();

        String[] sprints = path.split("/");
        String id = splits[1];
        orderService.delete(Long.parseLong(id));
    }

}