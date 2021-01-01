#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import streamlit as st
from fbprophet import Prophet
import databases.snowflake as snow
import looker
import altair as alt
import psycopg2
import snowflake.connector
import os
import sys
from datetime import datetime, date


# In[2]:


def predict_model(dataframe, interval_width = 0.95, changepoint_range = 0.8, weekly_seasonality = True):
    # Defines timeframe & tuning parameters
    m = Prophet(daily_seasonality = False, yearly_seasonality = True, weekly_seasonality = weekly_seasonality,
                #seasonality_mode = 'multiplicative', 
                interval_width = interval_width,
                changepoint_range = changepoint_range,
                changepoint_prior_scale = .11)
    # Fits model dataframe using parameters specified for seasonality, changepoints & interval width
    m = m.fit(dataframe)
    # Generates future date (ds) & measure (y, yhat, etc.) rows to be populated with predictions
    future_date = date.fromisoformat('2022-01-28')
    current_date = date.fromisoformat('2020-11-24')
    periods = (future_date - date.today()).days
    future = m.make_future_dataframe(periods=periods)

    # Predicts future values using calculated multiplicatve & additive terms:
    # trend * (1 + multiplicative_terms) + additive_terms
    forecast = m.predict(future)
    forecast['fact'] = dataframe['y'].reset_index(drop = True)
    # Creates a copy of the dataframe containing the target column list
    forecasted = forecast[['ds','trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()
    # Returns dataframe copy to be stored as method instance
    return forecasted


# In[3]:


# Method to plot the forecast predictions using Altair chart visualization
def plot_predict(forecasted, chart_title=''):
    # Specifies the chart values, color, titles, tooltips & interactive properties for confidence interval range
    interval = alt.Chart(forecasted).mark_area(interpolate="basis", color = '#7FC97F').encode(
    x=alt.X('ds',  title ='date'),
    y='yhat_upper',
    y2='yhat_lower',
    tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper', 'yhat']
    ).interactive().properties(
        title= chart_title + ' Snowflake Spend Forecast'
    )
    
    # Specifies the chart values, color, titles, tooltips & interactive properties for existing (realized actual) values
    # from historical dates
    fact = alt.Chart(forecasted).mark_circle(size=15, opacity=0.7, color = 'Black').encode(
        x='ds:T',
        y=alt.Y('fact', title='spend'),    
        tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper', 'yhat']
        )
    
    # Specifies the chart values, color, titles, tooltips & interactive properties for the forecasted values
    # of future dates
    predictions = alt.Chart(forecasted[forecasted['ds']>='2020-12-14']).mark_circle(size=15, opacity=0.7, color = 'Blue').encode(
        x='ds:T',
        y=alt.Y('yhat', title='spend'),    
        tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper', 'yhat']
        )

    # Returns method instance chart visualization for timeseries forecast
    return alt.layer(interval, fact, predictions)          .properties(width=870, height=450)          .configure_title(fontSize=20)


# In[4]:


# Retrieves values from snowflake_warehouse_cost relation
query = f'''
    SELECT 
       usage_day::DATE AS ds
       , sum(dollars_spent) as y
    FROM analytics.bizops.snowflake_warehouse_cost
    WHERE usage_day::DATE < CURRENT_DATE
    AND usage_day::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

# Generates dataframe and renames columns to required naming conventions specified by Prophet
df = snow.execute(query, as_df=True)
df = df.rename(columns={"DS":"ds", "Y": "y"})


# In[5]:


# Transforms dataframe, performs predictions, and stores output in variable "pred"
pred = predict_model(df)


# In[6]:


# Assigns title to streamlit webpage's generated chart tile
st.title('❄️Snowflake Spend Forecast')
# Generates the chart visualization and stores/overrides in variable "pred"
viz = plot_predict(pred)
# Spins up streamlit webpage with specified chart properties
st.write(viz)


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
next_fy = pred[pred['ds']>= '2020-12-01'].copy()
next_fy = next_fy.set_index('ds')
monthly = next_fy.groupby(pd.Grouper(freq="M")).sum()
fy = next_fy.sum()

st.title('❄️Snowflake Spend Forecast Monthly Dataframe')
st.write(monthly)

st.title('❄️Snowflake Spend Forecast FY22 Total Dataframe')
st.write(fy)

print(pred)




