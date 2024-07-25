#upsert 
from delta.tables import *

# Carregar tabelas Delta como DeltaTable
deltaTable_orders = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/datalake/delta/orders.delta")
deltaTable_order_details = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/datalake/delta/orderdetails.delta")

# Criar os novos registros que queremos inserir
new_order = spark.createDataFrame([(11078, "ALFKI", 1, "2023-08-01")], ["OrderID", "CustomerID", "EmployeeID", "OrderDate"])
new_order_details = spark.createDataFrame([(11078, 1, 18, 3)], ["OrderID", "ProductID", "UnitPrice", "Quantity"])

deltaTable_orders.alias("orders").merge(
    new_order.alias("newOrder"),
    "orders.OrderID = newOrder.OrderID")\
    .whenMatchedUpdate(set = {"CustomerID" : "newOrder.CustomerID", "EmployeeID" : "newOrder.EmployeeID", "OrderDate" : "newOrder.OrderDate"})\
    .whenNotMatchedInsert(values = {"OrderID" : "newOrder.OrderID", "CustomerID" : "newOrder.CustomerID", "EmployeeID" : "newOrder.EmployeeID", "OrderDate" : "newOrder.OrderDate"})\
    .execute()

deltaTable_order_details.alias("order_details").merge(
    new_order_details.alias("newOrderDetails"),
    "order_details.OrderID = newOrderDetails.OrderID AND order_details.ProductID = newOrderDetails.ProductID")\
    .whenMatchedUpdate(set = {"UnitPrice" : "newOrderDetails.UnitPrice", "Quantity" : "newOrderDetails.Quantity"})\
    .whenNotMatchedInsert(values = {"OrderID" : "newOrderDetails.OrderID", "ProductID" : "newOrderDetails.ProductID", "UnitPrice" : "newOrderDetails.UnitPrice", "Quantity" : "newOrderDetails.Quantity"})\
    .execute()