import cddp
import json
import os
import pandas as pd
import streamlit as st
import sys
import numpy as np


sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
import cddp.openai_api as openai_api
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.colored_header import colored_header
from streamlit_extras.chart_container import chart_container
if "current_pipeline_obj" not in st.session_state:
    switch_page("Home")

current_pipeline_obj = st.session_state["current_pipeline_obj"]

st.set_page_config(page_title="AI Assiatant")

colored_header(
    label="Pipeline Generator",
    description=f"Leverage AI to generate a pipeline for you!",
    color_name="violet-70",
)


def gen_use_cases():
    st.session_state['gen_pipeline_usecases'] = [
    {
      "UseCase": "Predictive Maintenance",
      "Description": "Use sensor data from machines and equipment to predict when maintenance is required, reducing unplanned downtime and extending equipment lifespan."
    },
    {
      "UseCase": "Quality Control",
      "Description": "Analyze data from sensors and cameras to detect defects in real-time during the production process, ensuring higher product quality and reducing waste."
    },
    {
      "UseCase": "Supply Chain Optimization",
      "Description": "Utilize data from suppliers, logistics, and demand forecasting to optimize inventory management, reduce lead times, and improve on-time deliveries."
    },
    {
      "UseCase": "Energy Management",
      "Description": "Monitor energy consumption in real-time and identify opportunities to reduce energy costs and environmental impact."
    },
    {
      "UseCase": "Production Planning",
      "Description": "Use historical data and demand forecasts to optimize production scheduling, resource allocation, and inventory levels."
    },
    {
      "UseCase": "Process Optimization",
      "Description": "Analyze data from manufacturing processes to identify bottlenecks, inefficiencies, and areas for improvement, leading to higher productivity and lower costs."
    },
    {
      "UseCase": "Quality Analytics",
      "Description": "Implement statistical process control (SPC) and data analytics to maintain consistent product quality and identify root causes of quality deviations."
    },
    {
      "UseCase": "Supplier Performance Analysis",
      "Description": "Evaluate supplier data to track the performance of various suppliers, ensuring that materials and components meet quality and delivery standards."
    },
    {
      "UseCase": "Inventory Management",
      "Description": "Implement just-in-time (JIT) inventory systems using data analytics to reduce excess inventory and associated carrying costs."
    },
    {
      "UseCase": "Labor Productivity",
      "Description": "Monitor employee performance and safety using wearable devices and analyze data to optimize work processes and reduce accidents."
    },
    {
      "UseCase": "Root Cause Analysis",
      "Description": "Investigate and address the root causes of production issues or defects by analyzing historical data and production records."
    },
    {
      "UseCase": "Regulatory Compliance",
      "Description": "Ensure compliance with industry and government regulations by tracking and reporting relevant data, such as emissions, safety records, and product traceability."
    },
    {
      "UseCase": "Demand Forecasting",
      "Description": "Use historical sales data, market trends, and external factors to create accurate demand forecasts, aiding in production planning and resource allocation."
    },
    {
      "UseCase": "Waste Reduction",
      "Description": "Analyze data to identify opportunities for reducing waste, whether in raw materials, energy consumption, or production processes."
    },
    {
      "UseCase": "Asset Tracking and Management",
      "Description": "Use RFID, GPS, or other tracking technologies to monitor the location and condition of assets, such as tools, equipment, and inventory."
    },
    {
      "UseCase": "Employee Training and Skill Development",
      "Description": "Use data analytics to assess employee skills and training needs, ensuring a skilled workforce capable of handling advanced manufacturing processes."
    }
  ]

SQL = [
    {
      "Statement": "SELECT name, salary FROM Employees;",
      "Description": "Retrieve all employees' names and salaries from the 'Employees' table."
    },
    {
      "Statement": "SELECT product_name, price FROM Products;",
      "Description": "Fetch the product names and their prices from the 'Products' table."
    },
    {
      "Statement": "SELECT customer_name, contact_email FROM Customers;",
      "Description": "Get a list of customer names and their contact emails from the 'Customers' table."
    },
    {
      "Statement": "SELECT order_number, order_date FROM Orders;",
      "Description": "Retrieve the order numbers and order dates from the 'Orders' table."
    },
    {
      "Statement": "SELECT book_title, author_name FROM Books;",
      "Description": "Fetch the book titles and authors' names from the 'Books' table."
    },
    {
      "Statement": "SELECT city_name, population FROM Cities;",
      "Description": "Get a list of cities and their populations from the 'Cities' table."
    },
    {
      "Statement": "SELECT category_name, COUNT(*) AS product_count FROM ProductCategories GROUP BY category_name;",
      "Description": "Retrieve the product categories and the number of products in each category from the 'ProductCategories' table."
    },
    {
      "Statement": "SELECT movie_title, release_year FROM Movies;",
      "Description": "Fetch the movie titles and release years from the 'Movies' table."
    },
    {
      "Statement": "SELECT c.customer_name, SUM(o.total_amount) AS total_purchase_amount FROM Customers AS c JOIN Orders AS o ON c.customer_id = o.customer_id GROUP BY c.customer_name;",
      "Description": "Get a list of customer names and their total purchase amounts from the 'Customers' and 'Orders' tables, joined by customer ID."
    },
    {
      "Statement": "SELECT e1.name AS employee_name, e2.name AS manager_name FROM Employees AS e1 LEFT JOIN Employees AS e2 ON e1.manager_id = e2.employee_id;",
      "Description": "Retrieve the employee names and their respective managers' names from the 'Employees' table (self-join)."
    }
  ]

industry_list = ["Other", "Airlines", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale"]
pipeline_industry = st.selectbox('Industry', industry_list, key='gen_industry')
pipeline_desc = st.text_area('Pipeline description', key='gen_pipeline_description')

tab_usecase, tab_stg, tab_std, tab_srv, tab_vis = st.tabs(['Use Cases', 'Staging', 'Standardization', 'Serving', 'Visualization'])


with tab_usecase: 
  if st.button("Generate Usecases"):
      gen_use_cases()
      pipeline_usecases = st.session_state['gen_pipeline_usecases']
      for usecase in pipeline_usecases:
          st.checkbox(usecase['UseCase'])
          st.write(usecase['Description'])

with tab_stg: 
  if st.button("Generate Sample Data"):
      for i in range(10):
        df = pd.DataFrame(
          np.random.randn(50, 10),
          columns=('col %d' % i for i in range(10)))
        st.checkbox('Dataset', key=f'gen_dataset_{i}')
        st.dataframe(df)  # Same as st.write(df)

with tab_std:
  if st.button("Generate Transformations"):
      for item in SQL:
        st.checkbox(item['Description'])
        st.code(item['Statement'])

with tab_srv:
  if st.button("Generate Serving"):
      for item in SQL:
        st.checkbox(item['Description'])
        st.code(item['Statement'])


with tab_vis:
  if st.button("Generate Visualization"):
      for i in range(10):
        chart_data = pd.DataFrame(
          np.random.randn(50, 10),
          columns=('col %d' % i for i in range(10)))
        st.checkbox(f'Chart {i}', key=f'gen_chart_{i}')
        rand = np.random.randint(0, 3)
        with chart_container(chart_data):
          if rand == 0:
            st.area_chart(chart_data)
          elif rand == 1:
             st.bar_chart(chart_data)
          elif rand == 2:
             st.line_chart(chart_data)




    

