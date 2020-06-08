<%@ page import="com.epam.model.Order" %>
<%@ page import="java.util.SortedMap" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <title>Title</title>
</head>

<body>
    <h2>Orders:</h2>
    <table border="1">
        <tr>
            <th>Id</th>
            <th>Name</th>
            <th>Id</th>
            <th>Phone number</th>
            <th>Ordered pizza</th>
            <th>Summary</th>
        </tr>
        <% SortedMap<Long, Order> orders = (SortedMap<Long, Order>) request.getAttribute("orders"); %>
        <%
            for (Long id : orders.keySet()) {
                Order order = orders.get(id);
        %>
        <tr>
            <td><%= order.getId() %>
            </td>
            <td><%= order.getCustomer().getName() %>
            </td>
            <td><%= order.getCustomer().getId() %>
            </td>
            <td><%= order.getCustomer().getPhoneNumber() %>
            </td>
            <td><%= order.getOrderedPizza().getName() %>
            </td>
            <td><%= order.getOrderedPizza().getPrice() %>
            </td>
        </tr>
        <%}%>
    </table>
</body>
</html>
