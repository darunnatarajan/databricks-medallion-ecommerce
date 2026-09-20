-- =====================================================================
-- MEDALLION ARCHITECTURE  |  LAYER 2 of 3  |  SILVER
-- =====================================================================
-- Purpose : Store CLEANED data. Same columns as bronze, no new columns.
-- Source  : e_commerce.bronze.*
-- Target  : e_commerce.silver.*
-- Rule    : Silver = CLEAN ONLY.
--           Same table names, same column names, same data types.
--           Only the VALUES change, never the structure.
--
-- Cleaning applied by the notebook (not by this DDL):
--   1. Trim extra spaces from all text columns
--   2. Convert empty text ('') to NULL
--   3. Standardise case  - email lowercase
--                        - gender and city capitalised
--                        - order_status and event_type lowercase
--   4. Remove duplicate rows based on the ID column
--   5. Remove rows where the ID column is NULL
--
-- The only structural difference from bronze: the ID column is NOT NULL
-- and marked as PRIMARY KEY, because silver guarantees it exists.
-- =====================================================================

-- Run this only if you want a clean restart:
-- DROP SCHEMA IF EXISTS e_commerce.silver CASCADE;

CREATE SCHEMA IF NOT EXISTS e_commerce.silver
  COMMENT 'Silver layer - cleaned data, same structure as bronze';

USE CATALOG e_commerce;
USE SCHEMA silver;


-- ---------------------------------------------------------------------
-- Table 1 : users
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.silver.users (
  user_id      STRING NOT NULL COMMENT 'User ID - cleaned, no duplicates',
  name         STRING          COMMENT 'Customer name - trimmed',
  email        STRING          COMMENT 'Email - trimmed and lowercase',
  gender       STRING          COMMENT 'Gender - capitalised',
  city         STRING          COMMENT 'City - trimmed and capitalised',
  signup_date  DATE            COMMENT 'Registration date',

  CONSTRAINT pk_silver_users PRIMARY KEY (user_id)
)
USING DELTA
COMMENT 'Silver - cleaned users data';


-- ---------------------------------------------------------------------
-- Table 2 : products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.silver.products (
  product_id    STRING NOT NULL COMMENT 'Product ID - cleaned, no duplicates',
  product_name  STRING          COMMENT 'Product name - trimmed',
  category      STRING          COMMENT 'Category - trimmed and capitalised',
  brand         STRING          COMMENT 'Brand - trimmed',
  price         DECIMAL(10,2)   COMMENT 'Price',
  rating        DECIMAL(3,2)    COMMENT 'Rating',

  CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
)
USING DELTA
COMMENT 'Silver - cleaned products data';


-- ---------------------------------------------------------------------
-- Table 3 : orders
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.silver.orders (
  order_id      STRING NOT NULL COMMENT 'Order ID - cleaned, no duplicates',
  user_id       STRING          COMMENT 'User who placed the order',
  order_date    TIMESTAMP       COMMENT 'Order date and time',
  order_status  STRING          COMMENT 'Status - trimmed and lowercase',
  total_amount  DECIMAL(12,2)   COMMENT 'Order total value',

  CONSTRAINT pk_silver_orders PRIMARY KEY (order_id)
)
USING DELTA
COMMENT 'Silver - cleaned orders data';


-- ---------------------------------------------------------------------
-- Table 4 : order_items
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.silver.order_items (
  order_item_id  STRING NOT NULL COMMENT 'Order item ID - cleaned, no duplicates',
  order_id       STRING          COMMENT 'Parent order',
  product_id     STRING          COMMENT 'Product purchased',
  user_id        STRING          COMMENT 'User who placed the order',
  quantity       INT             COMMENT 'Units purchased',
  item_price     DECIMAL(10,2)   COMMENT 'Price per unit',
  item_total     DECIMAL(12,2)   COMMENT 'Line total value',

  CONSTRAINT pk_silver_order_items PRIMARY KEY (order_item_id)
)
USING DELTA
COMMENT 'Silver - cleaned order items data';


-- ---------------------------------------------------------------------
-- Table 5 : reviews
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.silver.reviews (
  review_id    STRING NOT NULL COMMENT 'Review ID - cleaned, no duplicates',
  order_id     STRING          COMMENT 'Related order',
  product_id   STRING          COMMENT 'Product reviewed',
  user_id      STRING          COMMENT 'User who wrote the review',
  rating       INT             COMMENT 'Star rating 1 to 5',
  review_text  STRING          COMMENT 'Review comment - trimmed',
  review_date  TIMESTAMP       COMMENT 'Review date and time',

  CONSTRAINT pk_silver_reviews PRIMARY KEY (review_id)
)
USING DELTA
COMMENT 'Silver - cleaned reviews data';


-- ---------------------------------------------------------------------
-- Table 6 : events
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS e_commerce.silver.events (
  event_id         STRING NOT NULL COMMENT 'Event ID - cleaned, no duplicates',
  user_id          STRING          COMMENT 'User who triggered the event',
  product_id       STRING          COMMENT 'Product involved',
  event_type       STRING          COMMENT 'Event type - trimmed and lowercase',
  event_timestamp  TIMESTAMP       COMMENT 'Event date and time',

  CONSTRAINT pk_silver_events PRIMARY KEY (event_id)
)
USING DELTA
COMMENT 'Silver - cleaned events data';


-- ---------------------------------------------------------------------
-- Check the tables were created
-- ---------------------------------------------------------------------
SHOW TABLES IN e_commerce.silver;
