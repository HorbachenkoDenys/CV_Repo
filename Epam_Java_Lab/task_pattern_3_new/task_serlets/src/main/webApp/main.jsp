<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
     <title>Title</title>
    </head>
    <body>
    <h2>Fill the form to order pizza</h2>
    <form action="/orders" method="POST">
        <input type="text" placeholder="Name" name="Name"/>
        <br>
        <input type="text" placeholder="Id" name="Id"/>
        <br>
        <input type="text" placeholder="Phone number" name="phoneNumber"/>
        <br>
        <select name="pizza">
            <option value="peperoni">Cheese pizza</option>
            <option value="cheese">Cheese pizza</option>
             <option value="carbonara">Carbonara</option>
            <option value="vegan">Vegan pizza</option>

        </select>
        <br>
        <button type="submit">Submit</button>
    </form>
    </body>
    </html>

