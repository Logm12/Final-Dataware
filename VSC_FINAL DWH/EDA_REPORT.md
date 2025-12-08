# 📊 Supply Chain EDA Report

## 1. Sales Performance
- **Total Revenue:** $816,040.42
- **Total Profit:** $83,594.64
- **Avg Order Value:** $204.06

### Top 5 Best Selling Products
- Perfect Fitness Perfect Rip Deck: $118,360.27
- Smart watch: $117,006.75
- Nike Men's Free 5.0+ Running Shoe: $96,390.36
- Nike Men's Dri-FIT Victory Golf Polo: $95,400.00
- Nike Men's CJ Elite 2 TD Football Cleat: $83,583.57

## 2. Logistics Performance

### Delivery Status Distribution
- Late delivery: 53.86%
- Advance shipping: 23.16%
- Shipping on time: 17.48%
- Shipping canceled: 5.50%

- **Overall Late Delivery Risk:** 53.86%

## 3. Market Analysis

### Sales by Market
- Europe: $298,835.15
- Pacific Asia: $277,075.32
- LATAM: $151,755.93
- USCA: $48,741.25
- Africa: $39,632.77

Thông tin chi tiết:

**Details:** Dataset này là dữ liệu giao dịch chuỗi cung ứng (Supply Chain Transactional Data), ghi lại hành trình từ lúc khách đặt hàng đến khi giao hàng.

**Các nhóm thông tin chính:**

Thông tin Đơn hàng (Order Info): Order Id, Order Date, Order Status, Order Region, Market.
Thông tin Tài chính (Financials): Sales (Doanh thu), Benefit per order (Lợi nhuận), Product Price, Order Item Discount.
Thông tin Khách hàng (Customer Info): Customer Id, Customer Name, Customer Segment, Customer City/Country.
Thông tin Sản phẩm (Product Info): Product Name, Category Name, Product Image.
Thông tin Vận chuyển (Logistics): Shipping Mode (Standard, First Class...), Days for shipping (Real vs Scheduled), Delivery Status (Late, On time...), Late_delivery_risk.
Thông tin Địa lý (Location): Latitude, Longitude (của kho hoặc điểm giao hàng).

**2. Chiến lược chia thành 2 Service (Microservices)**
Với dữ liệu này, cách chia hợp lý nhất cho kiến trúc Microservices là tách biệt Quy trình Bán hàng (Sales) và Quy trình Vận hành (Operations/Logistics).

Tôi đã tách file gốc thành 2 file riêng biệt (đã lưu trong thư mục của bạn):

**Service 1: Sales & Order Service (_SERVICE_SALES.csv)**
Chức năng: Quản lý đơn hàng, khách hàng, sản phẩm và doanh thu.
Dữ liệu bao gồm:
Order Id (Key)
Thông tin khách hàng (Customer Id, Name, City...)
Thông tin sản phẩm (Product Name, Price, Category...)
Thông tin tài chính (Sales, Profit, Discount)
Mục đích: Phục vụ cho team Sales, Marketing, Customer Support (giai đoạn trước giao hàng).

**Service 2: Logistics & Fulfillment Service (_SERVICE_LOGISTICS.csv)**
Chức năng: Quản lý vận chuyển, kho bãi và giao nhận.
Dữ liệu bao gồm:
Order Id (Foreign Key - liên kết với Sales Service)
Thông tin vận chuyển (Shipping Mode, Shipping Date)
Hiệu suất giao hàng (Days Real vs Scheduled, Delivery Status)
Rủi ro (Late_delivery_risk)
Địa lý (Latitude, Longitude)
Mục đích: Phục vụ cho team Kho vận, Shipper, Quản lý chuỗi cung ứng.

**3. Chiến lược EDA (Exploratory Data Analysis)**

A. Phân tích Hiệu quả Kinh doanh (Sales Performance)
Doanh thu & Lợi nhuận: Tổng doanh thu là $816k, lợi nhuận $83k (Margin ~10%).
Top Sản phẩm: "Perfect Fitness Perfect Rip Deck" và "Smart watch" là 2 sản phẩm chủ lực (đóng góp ~$230k).
Action: Tập trung marketing và inventory cho top 5 sản phẩm này.
Thị trường: Châu Âu (Europe) và Châu Á Thái Bình Dương (Pacific Asia) là 2 thị trường lớn nhất.

B. Phân tích Chuỗi cung ứng (Logistics Performance)
Tỷ lệ giao trễ (Late Delivery Risk): Rất báo động! 53.86% đơn hàng bị giao trễ (Late delivery).
Insight: Hơn một nửa số đơn hàng gặp vấn đề vận chuyển.
Action: Cần điều tra ngay quy trình vận chuyển tại các kho hoặc đối tác shipping.
Trạng thái giao hàng: Chỉ có 17.48% là giao đúng hạn (Shipping on time).
