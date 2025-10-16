# Final-Dataware
Link: [DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis?resource=download)
🧩 1. Database Service A — Order Management System (OMS)
Focus: Handles everything from the customer order until the shipment is prepared and scheduled.
Includes columns related to:
Customer and Order details
Customer Id, Fname, Lname, Segment, City, State, Country, Street, Zipcode, Email
Order Id, Order Date (DateOrders), Order Status, Order City, Order State, Order Country, Order Zipcode
Order item details
Order Item Id, Order Item Product Price, Order Item Discount, Discount Rate, Quantity, Total, Profit, Profit Ratio
Product details
Product Card Id, Product Name, Product Description, Product Category Id, Product Price, Product Status, Product Image
Sales & Profit metrics
Sales per customer, Benefit per order, Sales, Order Profit per order
Category and Department
Category Id, Category Name, Department Id, Department Name
Market & Region
Market, Order Region
Shipment scheduling info
Days for shipment (scheduled)
Purpose:
This service manages everything up to the point where an order is ready for shipping — essentially the "pre-logistics" phase.

🚚 2. Database Service B — Shipping & Logistics Management System (SLMS)
Focus: Handles the movement, tracking, and performance of deliveries.
Includes columns related to:
Shipping performance
Shipping Mode
Shipping date (DateOrders)
Days for shipping (real)
Late_delivery_risk
Delivery Status
Location and routing
Latitude, Longitude
Customer City, Customer State, Customer Country (for destination)
Order linkage
Order Id (foreign key from OMS)
Order Customer Id (foreign key from OMS)
Purpose:
This service manages the “post-order” phase — logistics execution, tracking, carrier assignment, and delivery analytics.
New Directions - Business Solutions Using Essential and Advanced Algorithms
