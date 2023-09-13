import streamlit as st

def show_chart(chart_settings, chart_data):
    chart_type = chart_settings['type']
    if chart_type == 'Bar Chart' or chart_type == 'Line Chart' or chart_type == 'Area Chart' or chart_type == 'Scatter Chart':
        x_axis = chart_settings['x_axis']
        y_axis = chart_settings['y_axis']
        if chart_type == 'Scatter Chart':
            scatter_size = chart_settings['scatter_size']
    elif chart_type == 'Pie Chart':
        cate_axis = chart_settings['cate_axis']
        val_axis = chart_settings['val_axis']

    if chart_type == 'Bar Chart':
        st.bar_chart(chart_data, x=x_axis, y=y_axis)
    elif chart_type == 'Line Chart':
        st.line_chart(chart_data, x=x_axis, y=y_axis)                    
    elif chart_type == 'Area Chart':
        st.area_chart(chart_data,  x=x_axis, y=y_axis)                    
    elif chart_type == 'Scatter Chart':
        st.vega_lite_chart(chart_data, {
            'mark': {'type': 'circle', 'tooltip': True},
            'encoding': {
                'x': {'field': x_axis, "type": "nominal"},
                'y': {'field': y_axis, 'type': 'quantitative'},
                'size': {'field': scatter_size, 'type': 'quantitative'},
                'color': {'field': scatter_size, 'type': 'quantitative'},
            },
        },use_container_width=True)
    elif chart_type == 'Pie Chart':
        st.vega_lite_chart(chart_data, {
            "mark": {"type": "arc", "innerRadius": 70},
            "encoding": {
                "theta": {"field": val_axis, "type": "quantitative"},
                "color": {"field": cate_axis, "type": "nominal"}
            }
        },use_container_width=True)