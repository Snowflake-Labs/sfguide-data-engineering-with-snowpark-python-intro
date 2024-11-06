import sqlglot as sq
import re
# Example stored procedure SQL
sql ="""
CREATE PROCEDURE get_employee_details (IN emp_id INT)
BEGIN
    SELECT name, job_title, salary
    FROM employees
    WHERE employee_id = emp_id;
END;
"""
sql_query = re.sub(r"CREATE PROCEDURE.*?BEGIN\s+", "", sql, flags=re.DOTALL)
sql_query = re.sub(r"\s*END;", "", sql_query, flags=re.DOTALL)
print(sql_query)
# Parse the SQL statement to get the AST
parsed_query = sq.parse_one(sql_query)


# Function to extract tables involved in the stored procedure
def extract_tables_from_procedure(parsed_query):
    tables = set()
    
    # Walk through the parsed query to identify table references
    for node in parsed_query.walk():
        if isinstance(node, sq.expressions.Table):
            tables.add(node.name)
    
    return tables

# Extract tables involved in the stored procedure
tables = extract_tables_from_procedure(parsed_query)

# Print the tables
print("Tables involved in the stored procedure:", tables)
