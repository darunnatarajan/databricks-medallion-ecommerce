-- =====================================================================
-- MEDALLION ARCHITECTURE  |  LAYER 1 of 3  |  BRONZE
-- =====================================================================
-- Purpose : Store the source data exactly as it arrives. No cleaning.
-- Source  : CSV files in /Volumes/e_commerce/bronze/landing/
-- Target  : e_commerce.bronze.*
-- Rule    : Bronze = RAW. If the CSV has it, bronze keeps it.
-- =====================================================================

-- Run this only if you want a clean restart:
-- DROP SCHEMA IF EXISTS e_commerce.bronze CASCADE;

CREATE CATALOG IF NOT EXISTS e_commerce;

CREATE SCHEMA IF NOT EXISTS e_commerce.bronze
  COMMENT 'Bronze layer - raw data as received from source';

USE CATALOG e_commerce;
USE SCHEMA bronze;


-- ---------------------------------------------------------------------
-- Table 1 : users
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.bronze.users (
  user_id      STRING  COMMENT 'User ID from source',
  name         STRING  COMMENT 'Customer name',
  email        STRING  COMMENT 'Email address',
  gender       STRING  COMMENT 'Gender',
  city         STRING  COMMENT 'City',
  signup_date  DATE    COMMENT 'Registration date'
)
USING DELTA
COMMENT 'Bronze - raw users data';


-- ---------------------------------------------------------------------
-- Table 2 : products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.bronze.products (
  product_id    STRING         COMMENT 'Product ID from source',
  product_name  STRING         COMMENT 'Product name',
  category      STRING         COMMENT 'Product category',
  brand         STRING         COMMENT 'Brand name',
  price         DECIMAL(10,2)  COMMENT 'Product price',
  rating        DECIMAL(3,2)   COMMENT 'Product rating'
)
USING DELTA
COMMENT 'Bronze - raw products data';


-- ---------------------------------------------------------------------
-- Table 3 : orders
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.bronze.orders (
  order_id      STRING         COMMENT 'Order ID from source',
  user_id       STRING         COMMENT 'User who placed the order',
  order_date    TIMESTAMP      COMMENT 'Order date and time',
  order_status  STRING         COMMENT 'Order status',
  total_amount  DECIMAL(12,2)  COMMENT 'Order total value'
)
USING DELTA
COMMENT 'Bronze - raw orders data';


-- ---------------------------------------------------------------------
-- Table 4 : order_items
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.bronze.order_items (
  order_item_id  STRING         COMMENT 'Order item ID from source',
  order_id       STRING         COMMENT 'Parent order',
  product_id     STRING         COMMENT 'Product purchased',
  user_id        STRING         COMMENT 'User who placed the order',
  quantity       INT            COMMENT 'Units purchased',
  item_price     DECIMAL(10,2)  COMMENT 'Price per unit',
  item_total     DECIMAL(12,2)  COMMENT 'Line total value'
)
USING DELTA
COMMENT 'Bronze - raw order items data';


-- ---------------------------------------------------------------------
-- Table 5 : reviews
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.bronze.reviews (
  review_id    STRING     COMMENT 'Review ID from source',
  order_id     STRING     COMMENT 'Related order',
  product_id   STRING     COMMENT 'Product reviewed',
  user_id      STRING     COMMENT 'User who wrote the review',
  rating       INT        COMMENT 'Star rating 1 to 5',
  review_text  STRING     COMMENT 'Review comment',
  review_date  TIMESTAMP  COMMENT 'Review date and time'
)
USING DELTA
COMMENT 'Bronze - raw reviews data';


-- ---------------------------------------------------------------------
-- Table 6 : events
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.bronze.events (
  event_id         STRING     COMMENT 'Event ID from source',
  user_id          STRING     COMMENT 'User who triggered the event',
  product_id       STRING     COMMENT 'Product involved',
  event_type       STRING     COMMENT 'view, cart, wishlist or purchase',
  event_timestamp  TIMESTAMP  COMMENT 'Event date and time'
)
USING DELTA
COMMENT 'Bronze - raw events data';


-- ---------------------------------------------------------------------
-- Check the tables were created
-- ---------------------------------------------------------------------
SHOW TABLES IN e_commerce.bronze;
