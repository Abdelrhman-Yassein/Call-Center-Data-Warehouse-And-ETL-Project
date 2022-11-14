# Create DataBase Query
create_database = 'CREATE DATABASE IF NOT EXISTS call_center;'
# Drop Table Queries
# Drop Customers Dimension Table If Exists
drop_customer_table = "DROP TABLE IF EXISTS customers;"
# Drop call_time Dimension Table If Exists
drop_call_time_table = "DROP TABLE IF EXISTS call_time;"
# Drop call_info Dimension Table If Exists
drop_call_info_table = "DROP TABLE IF EXISTS call_info;"
# Drop call_fact Fact Table If Exists
drop_call_fact_table = "DROP TABLE IF EXISTS call_fact;"


# Create Table Queries
# Create Customers Dimension Table Query
create_customer_table = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     INT NOT NULL,
    customer_name   VARCHAR(255) NOT NULL,
    city            VARCHAR(50),
    state           VARCHAR(50),
    sentiment       VARCHAR(50),
    PRIMARY KEY (customer_id)
)
"""
# Create Call_time Dimension Table Query
create_call_time_table = """
CREATE TABLE IF NOT EXISTS call_time (
    call_date   DATE NOT NULL,
    day         INT NOT NULL,
    week        INT,
    weekday     VARCHAR(255),
    month       INT,
    year        INT,
    primary key (call_date)
)
"""

# Create call_info Dimension Table Query
create_call_info_table = """
CREATE TABLE IF NOT EXISTS call_info (
    call_id          VARCHAR(255) NOT NULL,
    reason           VARCHAR(255),
    channel          VARCHAR(255),
    call_center      VARCHAR(255),
    response_time    VARCHAR(255),
    PRIMARY KEY (call_id),
    CONSTRAINT fk_callFact FOREIGN KEY (call_id)
        REFERENCES call_fact (call_id)
        ON DELETE CASCADE
)
"""

# Create call_fact Fact Dimension Table Query
create_call_fact_table = """
CREATE TABLE IF NOT EXISTS call_fact (
    call_id          VARCHAR(255) NOT NULL,
    customer_id      INT NOT NULL,
    call_date        DATE NOT NULL,
    csat_score       INT NOT NULL,
    call_in_minutes  INT,
    PRIMARY KEY (call_id),
    CONSTRAINT fk_customer FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id),
    CONSTRAINT fk_call_date FOREIGN KEY (call_date)
        REFERENCES call_time (call_date)
)
"""
drop_tables_queries = [drop_customer_table, drop_call_time_table,
                       drop_call_info_table, drop_call_fact_table]
create_tables_queries = [create_customer_table, create_call_time_table,
                         create_call_fact_table, create_call_fact_table]
