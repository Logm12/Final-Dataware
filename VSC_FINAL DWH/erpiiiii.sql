-- =====================================================
-- CANIFA Fashion System - Database Creation Script
-- =====================================================

-- Drop existing database if exists (use with caution in production)
DROP DATABASE IF EXISTS canifa_fashion_db;
CREATE DATABASE canifa_fashion_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE canifa_fashion_db;

-- =====================================================
-- 1. USER Table
-- =====================================================
CREATE TABLE USER (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone VARCHAR(20),
    role ENUM('Admin', 'Sales') NOT NULL DEFAULT 'Sales',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_role (role),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB;

-- =====================================================
-- 2. CUSTOMER Table
-- =====================================================
CREATE TABLE CUSTOMER (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    phone_number VARCHAR(20) NOT NULL UNIQUE,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    membership_tier ENUM('Silver', 'Gold', 'Diamond') NOT NULL DEFAULT 'Silver',
    total_points INT NOT NULL DEFAULT 0,
    registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_phone (phone_number),
    INDEX idx_membership (membership_tier),
    CONSTRAINT chk_total_points CHECK (total_points >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 3. STORE Table
-- =====================================================
CREATE TABLE STORE (
    store_id INT AUTO_INCREMENT PRIMARY KEY,
    store_code VARCHAR(20) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    address VARCHAR(255) NOT NULL,
    city VARCHAR(50) NOT NULL,
    phone VARCHAR(20),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_store_code (store_code),
    INDEX idx_city (city),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB;

-- =====================================================
-- 4. PRODUCT Table
-- =====================================================
CREATE TABLE PRODUCT (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    barcode VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    description TEXT,
    material VARCHAR(100),
    category ENUM('Men', 'Women', 'Kids') NOT NULL,
    base_price DECIMAL(10, 2) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_sku (sku),
    INDEX idx_barcode (barcode),
    INDEX idx_category (category),
    INDEX idx_is_active (is_active),
    CONSTRAINT chk_base_price CHECK (base_price >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 5. PRODUCT_VARIANT Table
-- =====================================================
CREATE TABLE PRODUCT_VARIANT (
    variant_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    size VARCHAR(10) NOT NULL COMMENT 'S, M, L, XL, XXL',
    color VARCHAR(50) NOT NULL,
    variant_sku VARCHAR(50) NOT NULL UNIQUE,
    price_adjustment DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES PRODUCT(product_id) ON DELETE RESTRICT,
    INDEX idx_product_id (product_id),
    INDEX idx_variant_sku (variant_sku),
    INDEX idx_size_color (size, color),
    INDEX idx_is_active (is_active),
    UNIQUE KEY uk_product_size_color (product_id, size, color)
) ENGINE=InnoDB;

-- =====================================================
-- 6. INVENTORY Table
-- =====================================================
CREATE TABLE INVENTORY (
    inventory_id INT AUTO_INCREMENT PRIMARY KEY,
    variant_id INT NOT NULL,
    store_id INT NOT NULL,
    quantity_on_hand INT NOT NULL DEFAULT 0,
    reserved_quantity INT NOT NULL DEFAULT 0,
    available_quantity INT NOT NULL DEFAULT 0,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (variant_id) REFERENCES PRODUCT_VARIANT(variant_id) ON DELETE RESTRICT,
    FOREIGN KEY (store_id) REFERENCES STORE(store_id) ON DELETE RESTRICT,
    INDEX idx_variant_id (variant_id),
    INDEX idx_store_id (store_id),
    INDEX idx_available_quantity (available_quantity),
    UNIQUE KEY uk_variant_store (variant_id, store_id),
    CONSTRAINT chk_quantity_on_hand CHECK (quantity_on_hand >= 0),
    CONSTRAINT chk_reserved_quantity CHECK (reserved_quantity >= 0),
    CONSTRAINT chk_available_quantity CHECK (available_quantity >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 7. STOCK_ADJUSTMENT Table
-- =====================================================
CREATE TABLE STOCK_ADJUSTMENT (
    adjustment_id INT AUTO_INCREMENT PRIMARY KEY,
    inventory_id INT NOT NULL,
    user_id INT NOT NULL,
    adjustment_type ENUM('Initial', 'Addition', 'Deduction', 'Transfer') NOT NULL,
    quantity_change INT NOT NULL,
    reason VARCHAR(255),
    reference_number VARCHAR(50),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (inventory_id) REFERENCES INVENTORY(inventory_id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES USER(user_id) ON DELETE RESTRICT,
    INDEX idx_inventory_id (inventory_id),
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at),
    INDEX idx_adjustment_type (adjustment_type)
) ENGINE=InnoDB;

-- =====================================================
-- 8. STOCK_TRANSFER Table
-- =====================================================
CREATE TABLE STOCK_TRANSFER (
    transfer_id INT AUTO_INCREMENT PRIMARY KEY,
    from_store_id INT NOT NULL,
    to_store_id INT NOT NULL,
    user_id INT NOT NULL,
    status ENUM('Pending', 'In-Transit', 'Completed', 'Cancelled') NOT NULL DEFAULT 'Pending',
    transfer_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    notes TEXT,
    FOREIGN KEY (from_store_id) REFERENCES STORE(store_id) ON DELETE RESTRICT,
    FOREIGN KEY (to_store_id) REFERENCES STORE(store_id) ON DELETE RESTRICT,
    FOREIGN KEY (user_id) REFERENCES USER(user_id) ON DELETE RESTRICT,
    INDEX idx_from_store (from_store_id),
    INDEX idx_to_store (to_store_id),
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_transfer_date (transfer_date),
    CONSTRAINT chk_different_stores CHECK (from_store_id != to_store_id)
) ENGINE=InnoDB;

-- =====================================================
-- 9. STOCK_TRANSFER_ITEM Table
-- =====================================================
CREATE TABLE STOCK_TRANSFER_ITEM (
    transfer_item_id INT AUTO_INCREMENT PRIMARY KEY,
    transfer_id INT NOT NULL,
    variant_id INT NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (transfer_id) REFERENCES STOCK_TRANSFER(transfer_id) ON DELETE CASCADE,
    FOREIGN KEY (variant_id) REFERENCES PRODUCT_VARIANT(variant_id) ON DELETE RESTRICT,
    INDEX idx_transfer_id (transfer_id),
    INDEX idx_variant_id (variant_id),
    CONSTRAINT chk_transfer_quantity CHECK (quantity > 0)
) ENGINE=InnoDB;

-- =====================================================
-- 10. PROMOTION Table
-- =====================================================
CREATE TABLE PROMOTION (
    promotion_id INT AUTO_INCREMENT PRIMARY KEY,
    promotion_code VARCHAR(50) NOT NULL UNIQUE,
    promotion_name VARCHAR(100) NOT NULL,
    description TEXT,
    discount_type ENUM('Percentage', 'Fixed Amount') NOT NULL,
    discount_value DECIMAL(10, 2) NOT NULL,
    min_purchase_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    start_date DATETIME NOT NULL,
    end_date DATETIME NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_promotion_code (promotion_code),
    INDEX idx_dates (start_date, end_date),
    INDEX idx_is_active (is_active),
    CONSTRAINT chk_discount_value CHECK (discount_value > 0),
    CONSTRAINT chk_min_purchase CHECK (min_purchase_amount >= 0),
    CONSTRAINT chk_date_range CHECK (start_date < end_date)
) ENGINE=InnoDB;

-- =====================================================
-- 11. PROMOTION_PRODUCT Table
-- =====================================================
CREATE TABLE PROMOTION_PRODUCT (
    promotion_product_id INT AUTO_INCREMENT PRIMARY KEY,
    promotion_id INT NOT NULL,
    product_id INT NOT NULL,
    apply_to ENUM('Product', 'Category') NOT NULL DEFAULT 'Product',
    category_filter VARCHAR(50),
    FOREIGN KEY (promotion_id) REFERENCES PROMOTION(promotion_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES PRODUCT(product_id) ON DELETE CASCADE,
    INDEX idx_promotion_id (promotion_id),
    INDEX idx_product_id (product_id),
    UNIQUE KEY uk_promotion_product (promotion_id, product_id)
) ENGINE=InnoDB;

-- =====================================================
-- 12. TRANSACTION Table
-- =====================================================
CREATE TABLE `TRANSACTION` (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_number VARCHAR(50) NOT NULL UNIQUE,
    customer_id INT,
    user_id INT NOT NULL,
    store_id INT NOT NULL,
    promotion_id INT,
    transaction_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    subtotal DECIMAL(10, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    tax_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    total_amount DECIMAL(10, 2) NOT NULL,
    loyalty_points_earned INT NOT NULL DEFAULT 0,
    loyalty_points_used INT NOT NULL DEFAULT 0,
    status ENUM('Completed', 'Cancelled', 'Returned') NOT NULL DEFAULT 'Completed',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES CUSTOMER(customer_id) ON DELETE SET NULL,
    FOREIGN KEY (user_id) REFERENCES USER(user_id) ON DELETE RESTRICT,
    FOREIGN KEY (store_id) REFERENCES STORE(store_id) ON DELETE RESTRICT,
    FOREIGN KEY (promotion_id) REFERENCES PROMOTION(promotion_id) ON DELETE SET NULL,
    INDEX idx_transaction_number (transaction_number),
    INDEX idx_customer_id (customer_id),
    INDEX idx_user_id (user_id),
    INDEX idx_store_id (store_id),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_status (status),
    INDEX idx_store_date (store_id, transaction_date),
    CONSTRAINT chk_subtotal CHECK (subtotal >= 0),
    CONSTRAINT chk_discount CHECK (discount_amount >= 0),
    CONSTRAINT chk_tax CHECK (tax_amount >= 0),
    CONSTRAINT chk_total CHECK (total_amount >= 0),
    CONSTRAINT chk_points_earned CHECK (loyalty_points_earned >= 0),
    CONSTRAINT chk_points_used CHECK (loyalty_points_used >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 13. TRANSACTION_ITEM Table
-- =====================================================
CREATE TABLE TRANSACTION_ITEM (
    transaction_item_id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id INT NOT NULL,
    variant_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    line_total DECIMAL(10, 2) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES `TRANSACTION`(transaction_id) ON DELETE CASCADE,
    FOREIGN KEY (variant_id) REFERENCES PRODUCT_VARIANT(variant_id) ON DELETE RESTRICT,
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_variant_id (variant_id),
    CONSTRAINT chk_item_quantity CHECK (quantity > 0),
    CONSTRAINT chk_unit_price CHECK (unit_price >= 0),
    CONSTRAINT chk_item_discount CHECK (discount_amount >= 0),
    CONSTRAINT chk_line_total CHECK (line_total >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 14. PAYMENT Table
-- =====================================================
CREATE TABLE PAYMENT (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id INT NOT NULL,
    payment_method ENUM('Cash', 'Card', 'QR') NOT NULL,
    amount_paid DECIMAL(10, 2) NOT NULL,
    reference_number VARCHAR(100),
    payment_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES `TRANSACTION`(transaction_id) ON DELETE CASCADE,
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_payment_method (payment_method),
    INDEX idx_payment_date (payment_date),
    CONSTRAINT chk_amount_paid CHECK (amount_paid > 0)
) ENGINE=InnoDB;

-- =====================================================
-- 15. RETURN Table
-- =====================================================
CREATE TABLE `RETURN` (
    return_id INT AUTO_INCREMENT PRIMARY KEY,
    return_number VARCHAR(50) NOT NULL UNIQUE,
    transaction_id INT NOT NULL,
    customer_id INT,
    user_id INT NOT NULL,
    store_id INT NOT NULL,
    return_type ENUM('Return', 'Exchange') NOT NULL,
    refund_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    reason TEXT,
    status ENUM('Pending', 'Approved', 'Rejected', 'Completed') NOT NULL DEFAULT 'Pending',
    return_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at DATETIME,
    FOREIGN KEY (transaction_id) REFERENCES `TRANSACTION`(transaction_id) ON DELETE RESTRICT,
    FOREIGN KEY (customer_id) REFERENCES CUSTOMER(customer_id) ON DELETE SET NULL,
    FOREIGN KEY (user_id) REFERENCES USER(user_id) ON DELETE RESTRICT,
    FOREIGN KEY (store_id) REFERENCES STORE(store_id) ON DELETE RESTRICT,
    INDEX idx_return_number (return_number),
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_customer_id (customer_id),
    INDEX idx_return_date (return_date),
    INDEX idx_status (status),
    CONSTRAINT chk_refund_amount CHECK (refund_amount >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 16. RETURN_ITEM Table
-- =====================================================
CREATE TABLE RETURN_ITEM (
    return_item_id INT AUTO_INCREMENT PRIMARY KEY,
    return_id INT NOT NULL,
    variant_id INT NOT NULL,
    transaction_item_id INT NOT NULL,
    quantity INT NOT NULL,
    refund_amount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (return_id) REFERENCES `RETURN`(return_id) ON DELETE CASCADE,
    FOREIGN KEY (variant_id) REFERENCES PRODUCT_VARIANT(variant_id) ON DELETE RESTRICT,
    FOREIGN KEY (transaction_item_id) REFERENCES TRANSACTION_ITEM(transaction_item_id) ON DELETE RESTRICT,
    INDEX idx_return_id (return_id),
    INDEX idx_variant_id (variant_id),
    INDEX idx_transaction_item_id (transaction_item_id),
    CONSTRAINT chk_return_quantity CHECK (quantity > 0),
    CONSTRAINT chk_return_refund CHECK (refund_amount >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 17. LOYALTY_POINT Table
-- =====================================================
CREATE TABLE LOYALTY_POINT (
    point_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    transaction_id INT,
    points_earned INT NOT NULL DEFAULT 0,
    points_used INT NOT NULL DEFAULT 0,
    points_balance INT NOT NULL,
    transaction_type ENUM('Earn', 'Redeem', 'Adjustment') NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES CUSTOMER(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id) REFERENCES `TRANSACTION`(transaction_id) ON DELETE SET NULL,
    INDEX idx_customer_id (customer_id),
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_created_at (created_at),
    INDEX idx_transaction_type (transaction_type),
    CONSTRAINT chk_points_earned CHECK (points_earned >= 0),
    CONSTRAINT chk_points_used_loyalty CHECK (points_used >= 0),
    CONSTRAINT chk_points_balance CHECK (points_balance >= 0)
) ENGINE=InnoDB;

-- =====================================================
-- 18. AUDIT_LOG Table
-- =====================================================
CREATE TABLE AUDIT_LOG (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    action_type ENUM('Create', 'Update', 'Delete', 'Price Change', 'Stock Adjustment') NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id INT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    ip_address VARCHAR(45),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES USER(user_id) ON DELETE RESTRICT,
    INDEX idx_user_id (user_id),
    INDEX idx_action_type (action_type),
    INDEX idx_entity (entity_type, entity_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB;