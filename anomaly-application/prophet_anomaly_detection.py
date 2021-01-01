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


# In[2]:


def fit_predict_model(dataframe, interval_width = 0.8, changepoint_range = 0.8, weekly_seasonality = True):
    m = Prophet(daily_seasonality = False, yearly_seasonality = False, weekly_seasonality = weekly_seasonality,
                #seasonality_mode = 'multiplicative', 
                interval_width = interval_width,
                changepoint_range = changepoint_range)
    m = m.fit(dataframe)
    forecast = m.predict(dataframe)
    forecast['fact'] = dataframe['y'].reset_index(drop = True)
    return forecast

# In[3]:


def detect_anomalies(forecast):
    forecasted = forecast[['ds','trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()
    #forecast['fact'] = df['y']

    forecasted['anomaly'] = 0
    forecasted.loc[forecasted['fact'] > forecasted['yhat_upper'], 'anomaly'] = 1
    forecasted.loc[forecasted['fact'] < forecasted['yhat_lower'], 'anomaly'] = -1

    #anomaly importances
    forecasted['importance'] = 0
    forecasted.loc[forecasted['anomaly'] ==1, 'importance'] = \
        (forecasted['fact'] - forecasted['yhat_upper'])/forecast['fact']
    forecasted.loc[forecasted['anomaly'] ==-1, 'importance'] = \
        (forecasted['yhat_lower'] - forecasted['fact'])/forecast['fact']
    
    return forecasted


# In[4]:


def plot_anomalies(forecasted, chart_title=''):
    interval = alt.Chart(forecasted).mark_area(interpolate="basis", color = '#7FC97F').encode(
    x=alt.X('ds:T',  title ='date'),
    y='yhat_upper',
    y2='yhat_lower',
    tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper']
    ).interactive().properties(
        title= chart_title + ' Anomaly Detection'
    )

    fact = alt.Chart(forecasted[forecasted.anomaly==0]).mark_circle(size=15, opacity=0.7, color = 'Black').encode(
        x='ds:T',
        y=alt.Y('fact', title='sales'),    
        tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper']
    ).interactive()

    anomalies = alt.Chart(forecasted[forecasted.anomaly!=0]).mark_circle(size=30, color = 'Red').encode(
        x='ds:T',
        y=alt.Y('fact', title='sales'),    
        tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper'],
        size = alt.Size( 'importance', legend=None)
    ).interactive()

    return alt.layer(interval, fact, anomalies)\
              .properties(width=870, height=450)\
              .configure_title(fontSize=20)

# In[8]:
def predict_model(dataframe, interval_width = 0.7, changepoint_range = 0.8, weekly_seasonality = True):
    m = Prophet(daily_seasonality = False, yearly_seasonality =True, weekly_seasonality = weekly_seasonality,
                #seasonality_mode = 'multiplicative', 
                interval_width = interval_width,
                changepoint_range = changepoint_range)
    m = m.fit(dataframe)
    future = m.make_future_dataframe(periods=365)

    forecast = m.predict(future)
    forecast['fact'] = dataframe['y'].reset_index(drop = True)
    forecasted = forecast[['ds','trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()
    return forecasted

def plot_predict(forecasted, chart_title='Test'):
    interval = alt.Chart(forecasted).mark_area(interpolate="basis", color = '#7FC97F').encode(
    x=alt.X('ds',  title ='date'),
    y='yhat_upper',
    y2='yhat_lower',
    tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper', 'yhat']
    ).interactive().properties(
        title= chart_title + ' Snowflake Spend Forecast'
    )
    
    fact = alt.Chart(forecasted).mark_circle(size=15, opacity=0.7, color = 'Black').encode(
        x='ds:T',
        y=alt.Y('fact', title='spend'),    
        tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper', 'yhat']
        )

    predictions = alt.Chart(forecasted[forecasted['ds']>='2020-11-19']).mark_circle(size=15, opacity=0.7, color = 'Blue').encode(
        x='ds:T',
        y=alt.Y('yhat', title='spend'),    
        tooltip=['ds', 'fact', 'yhat_lower', 'yhat_upper', 'yhat']
        )

    return alt.layer(interval, fact, predictions)\
          .properties(width=870, height=450)\
          .configure_title(fontSize=20)

query = f'''
    SELECT 
       usage_day::DATE AS ds
       , sum(dollars_spent) as y
    FROM analytics.bizops.snowflake_warehouse_cost
    WHERE usage_day::DATE < CURRENT_DATE
    AND usage_day::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)
df = df.rename(columns={"DS":"ds", "Y": "y"})

pred= predict_model(df)
st.title('❄️ Snowflake Spend Forecast')
pred = plot_predict(pred)
st.write(pred)
# query = f'''
#     SELECT 
#        timestamp::DATE AS ds
#        , count(*) as y
#     FROM analytics.events.user_events_telemetry
#     WHERE TIMESTAMP::DATE < CURRENT_DATE
#     AND TIMESTAMP::DATE >= '2018-02-01'
#     AND _dbt_source_relation2 IN ('"ANALYTICS".EVENTS.PORTAL_EVENTS')
#     GROUP BY 1
#     ORDER BY 1;'''

# df = snow.execute(query, as_df=True)

# df = df.rename(columns={"DS":"ds", "Y": "y"})

# pred = fit_predict_model(df)

# pred = detect_anomalies(pred)

# plot_anomalies(pred, chart_title='PORTAL EVENTS')
query = f'''
    SELECT 
       timestamp::DATE AS ds
       , count(*) as y
    FROM raw.blapi.usage_events
    WHERE timestamp::DATE < CURRENT_DATE
    AND timestamp::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('📈 Usage Event Volume Anomaly Detection')

server = plot_anomalies(pred, chart_title='BLApi Usage Event Volume')
st.write(server)

query = f'''
    SELECT 
       timestamp::DATE AS ds
       , sum(active_users) as y
    FROM raw.blapi.usage_events
    WHERE timestamp::DATE < CURRENT_DATE
    AND timestamp::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('📈 Usage Event Active Users Anomaly Detection')

server = plot_anomalies(pred, chart_title='BLApi Usage Event Active Users')
st.write(server)


query = f'''
    SELECT 
       updated_at::DATE AS ds
       , count(*) as y
    FROM raw.blapi.invoices_version
    WHERE updated_at::DATE <= CURRENT_DATE
    AND updated_at::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('🧾 Invoice Update Anomaly Detection')

server = plot_anomalies(pred, chart_title='BLApi Invoice Update Volume')
st.write(server)


query = f'''
    SELECT 
       created_at::DATE AS ds
       , count(*) as y
    FROM raw.blapi.invoices
    WHERE created_at::DATE < CURRENT_DATE
    AND created_at::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('🧾 Invoice Creation Anomaly Detection')

server = plot_anomalies(pred, chart_title='BLApi Invoice Creation Volume')
st.write(server)


# # In[10]:

query = f'''
    SELECT 
       date::DATE AS ds
       , count(*) as y
    FROM analytics.mattermost.server_daily_details_ext
    WHERE date::DATE < CURRENT_DATE
    AND date::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('🖥 Server Details Ext Anomaly Detection')

server = plot_anomalies(pred, chart_title='Server Details Extended')
st.write(server)

query = f'''
    SELECT 
       date::DATE AS ds
       , count(*) as y
    FROM analytics.mattermost.server_daily_details
    WHERE date::DATE < CURRENT_DATE
    AND date::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('🖥 Server Details Anomaly Detection')

server = plot_anomalies(pred, chart_title='Server Details')
st.write(server)


query = f'''
    SELECT 
       date::DATE AS ds
       , count(*) as y
    FROM analytics.staging.server_security_details
    WHERE date::DATE < CURRENT_DATE
    AND date::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('🖥 Server Security Details Anomaly Detection')

server = plot_anomalies(pred, chart_title='Server Security Details')
st.write(server) 

query = f'''
    SELECT 
       date::DATE AS ds
       , count(*) as y
    FROM analytics.staging.server_server_details
    WHERE date::DATE <= CURRENT_DATE
    AND date::DATE >= '2018-02-01'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('🖥 Server Server Details Anomaly Detection')

server = plot_anomalies(pred, chart_title='Server Server Details')
st.write(server) 


query = f'''
    SELECT 
       timestamp::DATE AS ds
       , count(*) as y
    FROM analytics.events.user_events_telemetry
    WHERE TIMESTAMP::DATE < CURRENT_DATE
    AND TIMESTAMP::DATE >= '2018-02-01'
    AND _dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS', '"ANALYTICS".EVENTS.SEGMENT_MOBILE_EVENTS')
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('📱 Mobile Anomaly Detection')

mobile = plot_anomalies(pred, chart_title='Mobile Events')
st.write(mobile)


# # In[12]:


query = f'''
    SELECT 
       timestamp::DATE AS ds
       , count(*) as y
    FROM analytics.events.user_events_telemetry
    WHERE TIMESTAMP::DATE < CURRENT_DATE
    AND TIMESTAMP::DATE >= '2020-05-01'
    AND _dbt_source_relation2 IN ('"ANALYTICS".EVENTS.RUDDER_WEBAPP_EVENTS')
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('💻 Rudder WebApp Events Anomaly Detection')

webapp = plot_anomalies(pred, chart_title='Rudder WebApp Events')
st.write(webapp)

query = f'''
    SELECT 
       timestamp::DATE AS ds
       , count(*) as y
    FROM analytics.events.user_events_telemetry
    WHERE TIMESTAMP::DATE < CURRENT_DATE
    AND TIMESTAMP::DATE >= '2020-05-01'
    AND _dbt_source_relation2 IN ('"ANALYTICS".EVENTS.SEGMENT_WEBAPP_EVENTS')
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('💻 Segment WebApp Events Anomaly Detection')

webapp = plot_anomalies(pred, chart_title='Rudder WebApp Events')
st.write(webapp)

query = f'''
    SELECT 
       w.timestamp::DATE AS ds
       , count(distinct w.anonymous_id) as y
    FROM analytics.web.daily_website_traffic w
    join analytics.web.user_agent_registry u
        on u.context_useragent = w.context_useragent
    WHERE w.TIMESTAMP::DATE < CURRENT_DATE
    AND w.TIMESTAMP::DATE >= '2020-05-01'
    AND NOT CASE WHEN CASE WHEN u.device_type IS NULL THEN 'Other' ELSE u.device_type END = 'Spider' THEN TRUE ELSE FALSE END 
    -- AND w._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.SEGMENT_WEBAPP_EVENTS')
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('💻 Website Unique Visitors Anomaly Detection')

webapp = plot_anomalies(pred, chart_title='Website Unique Visitors')
st.write(webapp)

query = f'''
    SELECT 
       timestamp::DATE AS ds
       , count(*) as y
    FROM analytics.web.daily_website_traffic
    WHERE TIMESTAMP::DATE < CURRENT_DATE
    AND TIMESTAMP::DATE >= '2020-05-01'
    -- AND _dbt_source_relation2 IN ('"ANALYTICS".EVENTS.SEGMENT_WEBAPP_EVENTS')
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('💻 Website Pageview Anomaly Detection')

webapp = plot_anomalies(pred, chart_title='Website Unique Pageview')
st.write(webapp)

query = f'''
    SELECT 
       date::DATE AS ds
       , avg(score) as y
    FROM analytics.mattermost.nps_user_daily_score
    WHERE date::DATE < CURRENT_DATE
    and user_role = 'user'
    GROUP BY 1
    ORDER BY 1;'''

df = snow.execute(query, as_df=True)

df = df.rename(columns={"DS":"ds", "Y": "y"})

pred = fit_predict_model(df)

pred = detect_anomalies(pred)

st.title('💻 NPS End User Satisfaction Score Anomaly Detection')

nps = plot_anomalies(pred, chart_title='Average NPS Score')
st.write(nps)

# plot_anomalies(pred, chart_title='Desktop/WebApp Events')


# # In[13]:


# query = f'''
#     SELECT 
#        timestamp::DATE AS ds
#        , count(*) as y
#     FROM raw.mm_telemetry_prod.server
#     WHERE TIMESTAMP::DATE < CURRENT_DATE
#     AND TIMESTAMP::DATE >= '2020-05-01'
#     GROUP BY 1
#     ORDER BY 1;'''

# df = snow.execute(query, as_df=True)

# df = df.rename(columns={"DS":"ds", "Y": "y"})

# pred = fit_predict_model(df)

# pred = detect_anomalies(pred)

# plot_anomalies(pred, chart_title='Server (Config Diagnostics) Telemetry')


# # In[14]:


# query = f'''
#     SELECT 
#        timestamp::DATE AS ds
#        , count(*) as y
#     FROM raw.mattermostcom.pages
#     WHERE TIMESTAMP::DATE < CURRENT_DATE
#     AND TIMESTAMP::DATE >= '2020-05-01'
#     GROUP BY 1
#     ORDER BY 1;'''

# df = snow.execute(query, as_df=True)

# df = df.rename(columns={"DS":"ds", "Y": "y"})

# pred = fit_predict_model(df)

# pred = detect_anomalies(pred)

# plot_anomalies(pred, chart_title='www.mattermost.com')


# # In[15]:


# query = f'''
#     SELECT 
#        timestamp::DATE AS ds
#        , count(*) as y
#     FROM raw.portal_prod.pages
#     WHERE TIMESTAMP::DATE < CURRENT_DATE
#     AND TIMESTAMP::DATE >= '2020-05-01'
#     GROUP BY 1
#     ORDER BY 1;'''

# df = snow.execute(query, as_df=True)

# df = df.rename(columns={"DS":"ds", "Y": "y"})

# pred = fit_predict_model(df)

# pred = detect_anomalies(pred)

# plot_anomalies(pred, chart_title='customers.mattermost.com')


# # In[18]:


# query = f'''
#     SELECT 
#        (LOGDATE || ' ' || LOGTIME)::DATE AS ds
#        , count(*) as y
#     FROM raw.releases.log_entries
#     WHERE (LOGDATE || ' ' || LOGTIME)::DATE < CURRENT_DATE
#     AND (LOGDATE || ' ' || LOGTIME)::DATE >= '2018-02-01'
#     GROUP BY 1
#     ORDER BY 1;'''

# df = snow.execute(query, as_df=True)

# df = df.rename(columns={"DS":"ds", "Y": "y"})

# pred = fit_predict_model(df)

# pred = detect_anomalies(pred)

# plot_anomalies(pred, chart_title='Downloads')


# # In[20]:


# query = f'''
#     SELECT 
#        timestamp::DATE AS ds
#        , count(*) as y
#     FROM raw.mattermost_nps.nps_feedback
#     WHERE TIMESTAMP::DATE < CURRENT_DATE
#     AND TIMESTAMP::DATE >= '2017-02-01'
#     GROUP BY 1
#     ORDER BY 1;'''

# df = snow.execute(query, as_df=True)

# df = df.rename(columns={"DS":"ds", "Y": "y"})

# pred = fit_predict_model(df)

# pred = detect_anomalies(pred)

# plot_anomalies(pred, chart_title='NPS Feedback')


# m = Prophet(growth='linear',n_changepoints=10, changepoint_range=.8, changepoint_prior_scale=0.01, weekly_seasonality=True, yearly_seasonality=True)
# m.fit(df)

# future = m.make_future_dataframe(periods=365)
# future

# forecast = m.predict(future)
# forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
# 

# help(m.plot)

# forecast

# from fbprophet.plot import add_changepoints_to_plot
# fig = m.plot(forecast, figsize=(15,10))
# a = add_changepoints_to_plot(fig.gca(), m, forecast)

# fig2 = m.plot_components(forecast)
