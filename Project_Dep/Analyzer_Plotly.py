import sqlglot as sq
import streamlit as st
import re
import networkx as nx
import plotly.graph_objects as go

# Function to extract tables and columns from the AST
def extract_tables(parsed_query):
    tables = set()
    columns = set()
    parent_object = None

    for node in parsed_query.walk():
        if isinstance(node, sq.expressions.Create):
            parent_object = node.this
        if isinstance(node, sq.expressions.Table):
            tables.add(node.name)
        # Uncomment if you want to add columns to the extracted information
        # elif isinstance(node, sq.expressions.Column):
        #     column_name = node.name
        #     table_name = node.table
        #     columns.add((table_name, column_name))
    return parent_object, tables

def read_query_from_path(file):
    with open(file, 'r') as f:
        query = f.read()
        return sq.parse_one(query)

# Define your SQL query file path
file = '/workspaces/sfguide-data-engineering-with-snowpark-python-intro/Project Dep/view_demo.sql'

# Read and parse the query
parsed_query = read_query_from_path(file)

# Extract parent (view name) and tables
parent, tables = extract_tables(parsed_query)

# Print extracted parent and tables for verification
print("Parent:", parent)
print("Tables:", tables)

# Function to create a hierarchy graph using NetworkX
def create_hierarchy_graph(parent, tables):
    G = nx.DiGraph()
    # Add the parent node (view)
    G.add_node(parent)
    
    # Add the child nodes (tables) and establish the relationships
    for table in tables:
        G.add_node(table)
        G.add_edge(parent, table)  # Connect the parent to each table
    
    return G

# Create the graph using NetworkX
G = create_hierarchy_graph(parent, tables)

# Convert the NetworkX graph to a format compatible with Plotly
def networkx_to_plotly(G):
    pos = nx.spring_layout(G, seed=42)  # You can experiment with different layouts
    node_x, node_y = [], []
    for node, (x, y) in pos.items():
        node_x.append(x)
        node_y.append(y)
    
    edge_x, edge_y = [], []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_y.append(y0)
        edge_y.append(y1)
    
    return node_x, node_y, edge_x, edge_y

# Convert to Plotly data
node_x, node_y, edge_x, edge_y = networkx_to_plotly(G)

# Create the edge trace (lines)
edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=0.5, color='#888'),
    hoverinfo='none',
    mode='lines'
)

# Create the node trace (points)
node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers+text',
    hoverinfo='text',
    marker=dict(
        showscale=True,
        colorscale='YlGnBu',  # Change this to another color scale if you prefer
        size=50,
        colorbar=dict(thickness=15, title='Node Connections', xanchor='left', titleside='right')
    )
)

# Add node labels (table names)
node_trace.text = [node for node in G.nodes()]

# **Corrected part**: Map the color values to a range from 0 to 1
node_trace.marker.color = [i / len(G.nodes()) for i in range(len(G.nodes()))]  # Map the nodes to a color scale

# Create the layout for the plot
layout = go.Layout(
    title="Hierarchy of v_department_summary and Related Tables",
    showlegend=False,
    hovermode='closest',
    margin=dict(b=0, l=0, r=0, t=40),
    xaxis=dict(showgrid=False, zeroline=False),
    yaxis=dict(showgrid=False, zeroline=False),
    titlefont_size=16
)

# Create the Plotly figure
fig = go.Figure(data=[edge_trace, node_trace], layout=layout)

# Show the interactive graph
fig.show()
