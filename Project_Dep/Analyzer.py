import sqlglot as sq
import streamlit as st
import re
import networkx as nx
import matplotlib.pyplot as plt
# from sqlglot import expressions 
# Function to extract tables and columns from the AST
parent_object=''
def extract_tables(parsed_query):
    tables = set()
    columns = set()
    for node in parsed_query.walk():
        if isinstance(node,sq.expressions.Create):
            parent_object=node.this
        if isinstance(node, sq.expressions.Table):
            tables.add(node.name)
        # elif isinstance(node, sq.expressions.Column):
    #     # node.this is the column name
    #     # node.table is the table name (if specified)
            # column_name = node.name
            # table_name = node.table
            # columns.add((table_name, column_name))
    return parent_object,tables
# ,columns

def read_query_from_path(file):
    with open(file,'r') as f:
        query=f.read()
        # print(query)
        return sq.parse_one(query)
    
# file='/workspaces/sfguide-data-engineering-with-snowpark-python-intro/Project Dep/view_demo.sql'
file='/workspaces/sfguide-data-engineering-with-snowpark-python-intro/Project Dep/proc.sql'
# Extract tables and columns
read_query_from_path(file)

parent,tables= extract_tables(read_query_from_path(file))
# ,columns
print("Parent:",parent) 
print("Tables:", tables)

dependencies=[]

def create_hierarchy_graph(parent, tables):
    # Create a directed graph (DiGraph)
    G = nx.DiGraph()

    # Add the parent node (view)
    G.add_node(parent)

    # Add the child nodes (tables) and establish the relationships
    for table in tables:
        G.add_node(table)
        G.add_edge(parent, table)  # Connect the parent to each table

    return G
print(create_hierarchy_graph(parent=parent,tables=tables))

G = create_hierarchy_graph(parent, tables)
pos = nx.spring_layout(G, seed=42)  # You can experiment with different layouts like shell_layout, etc.

# Draw the graph using matplotlib
plt.figure(figsize=(8, 6))
nx.draw(G, pos, with_labels=True, node_size=3000, node_color='skyblue', font_size=10, font_color='black', font_weight='bold', edge_color='gray')
plt.title("Hierarchy of v_department_summary and Related Tables")
# plt.show()
# name=parent.this.this+'.jpg'
# plt.savefig(name)
st.pyplot(plt)
# print(parent.this.this)