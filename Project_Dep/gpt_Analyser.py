import sqlglot
import graphviz

class SQLDependencyExtractor:
    def __init__(self, sql_query):
        self.sql_query = sql_query
        self.dependencies = {"tables": {}, "lineage": []}

    def parse_sql(self):
        """Parse the SQL query into an Abstract Syntax Tree (AST)."""
        # try:
            # Parse the SQL query using SQLGlot
        return sqlglot.parse_one(self.sql_query)
        # except  SqlglotError as e:
        #     raise Exception(f"Error parsing SQL: {e}")

    def extract_dependencies(self, parsed):
        """Extract table and column dependencies using SQLGlot walk()."""
        # print('====>',node.type)
        def visitor(node):
            # Check for table references
            # print('====>',node.type)
            if node.type in ['table', 'from', 'join']:
                print(node.name)
                table_name = node.name
                if table_name and table_name not in self.dependencies['tables']:
                    self.dependencies['tables'][table_name] = []

            # Check for column references
            if node.type == 'column':
                table_name = node.find_parent("table")
                column_name = node.name
                print(table_name.name,'=>',node.name)
                if table_name and table_name.name:
                    table_name_str = table_name.name
                    if table_name_str not in self.dependencies['tables']:
                        self.dependencies['tables'][table_name_str] = []
                    self.dependencies['tables'][table_name_str].append(column_name)
                    self.dependencies['lineage'].append({
                        "table": table_name_str,
                        "column": column_name
                    })

        # Walk the AST to extract tables and columns
        sqlglot.Expression.walk(parsed)

    # def generate_digraph(self, output_file="table_column_hierarchy"):
    #     """Generate a hierarchy diagram (Digraph) to visualize table-column dependencies."""
    #     graph = graphviz.Digraph(comment='Table-Column Dependency Graph')

    #     # Add table nodes
    #     for table in self.dependencies['tables']:
    #         graph.node(table, table)

    #     # Add column dependencies (edges)
    #     for lineage in self.dependencies['lineage']:
    #         graph.edge(lineage['table'], f"{lineage['table']}.{lineage['column']}")

    #     # Save and render the graph as a PNG file
    #     graph.render(output_file, format='png')

    def get_dependencies(self):
        """Get the table and column dependencies."""
        parsed = self.parse_sql()
        print(parsed)
        self.extract_dependencies(parsed)
        return self.dependencies

    # def display_dependencies(self):
    #     """Display the extracted dependencies in a readable format."""
    #     print("Tables and Columns:")
    #     for table, columns in self.dependencies["tables"].items():
    #         print(f"Table: {table}")
    #         for column in columns:
    #             print(f"  - Column: {column}")

    #     print("\nLineage (Table -> Column):")
    #     for line in self.dependencies["lineage"]:
    #         print(f"Table: {line['table']}, Column: {line['column']}")

# Example SQL query
sql_query = """
CREATE OR REPLACE VIEW v_department_summary AS
SELECT
    d.department_id,
    d.department_name,
    d.manager_id,
    l.city AS department_location,
    c.country_name,
    r.region_name,
    COUNT(e.employee_id) AS total_employees,
    AVG(e.salary) AS avg_salary,
    LISTAGG(j.job_title, ', ') WITHIN GROUP (ORDER BY j.job_title) AS job_titles
FROM
    employees e
JOIN
    departments d ON e.department_id = d.department_id
JOIN
    locations l ON d.location_id = l.location_id
JOIN
    countries c ON l.country_id = c.country_id
JOIN
    regions r ON c.region_id = r.region_id
JOIN
    jobs j ON e.job_id = j.job_id
GROUP BY
    d.department_id, d.department_name, d.manager_id, l.city, c.country_name, r.region_name
ORDER BY
    d.department_name;
"""

# Extract and display the dependencies
extractor = SQLDependencyExtractor(sql_query)
print(extractor.get_dependencies())
# dependencies = extractor.get_dependencies()

# Display the dependencies
# extractor.display_dependencies()

# Generate and render the Digraph to visualize the table-column hierarchy
# extractor.generate_digraph(output_file="table_column_hierarchy")
# print("Dependency graph generated and saved as 'table_column_hierarchy.png'")
