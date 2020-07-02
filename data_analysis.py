#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#First part ----import data to SQL server--------------

import pandas as pd
import pyodbc

#------------------Read csv file---------
                       
data = pd.read_csv(r'C:\Users\Shu\Downloads\dataset\purchases.csv')      
df = pd.DataFrame(data)
df.head()

data1 = pd.read_csv(r'C:\Users\Shu\Downloads\dataset\messages.csv')      
df1 = pd.DataFrame(data1)

data2 = pd.read_csv(r'C:\Users\Shu\Downloads\dataset\users.csv')      
df2 = pd.DataFrame(data2)

#--------Connect to SQL server---------
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                     'Server=DESKTOP-H5VPCDO\SS17;'
                     'Database=test_db;'
                     'Trusted_Connection=yes;')
cursor = conn.cursor()

#-------Create Table in SQL server-------
cursor.execute('Create Table dbo.purchases(user_id Varchar(50), purchase_date Date, purchase_count int)')
cursor.execute('Create Table dbo.messages(user_id Varchar(50), message_date Date, message_count int)')
cursor.execute('Create Table dbo.users(user_id Varchar(50), signup_date Date)')

#-------Insert Data to Table---------
for row in df.itertuples():
    cursor.execute('''
                insert into Test_DB.dbo.purchases
                values(?,?,?)
                    ''',
                  row.user_id,
                  row.purchase_date,
                  row.purchase_count)
    
for row in df1.itertuples():
    cursor.execute('''
                insert into Test_DB.dbo.messages
                values(?,?,?)
                    ''',
                  row.user_id,
                  row.message_date,
                  row.message_count) 
    
for row in d2.itertuples():
    cursor.execute('''
                insert into Test_DB.dbo.users
                values(?,?)
                    ''',
                  row.user_id,
                  row.signup_date
                 )   
        
conn.commit()
cursor.close()
conn.close()

#----------------------------------------------------------------------------
# Second Part ----------------------Data Cross Validation--------------------
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
#------------read csv file-----------------------------
data = pd.read_csv(r'C:\Users\Shu\Downloads\dataset\purchases.csv',parse_dates=True)      
df_pur = pd.DataFrame(data)
df_pur.head()

data1 = pd.read_csv(r'C:\Users\Shu\Downloads\dataset\messages.csv',parse_dates=True)      
df_mes = pd.DataFrame(data1)
df_mes.head()

data2 = pd.read_csv(r'C:\Users\Shu\Downloads\dataset\users.csv',parse_dates=True)      
df_users = pd.DataFrame(data2)
#print(df_users)
df_users.describe()

#Data clean------remove NA values from users table
df_users1=df_users.dropna()
df_users1.describe()    
# total user (X) is 23841 as same as SQL query result.  

# prepare purchases and users data
user_id_purchase_df = df_pur[["user.id", "purchase.date"]]
user_id_signup_df = df_users1[["user.id","signup.date"]]

# merge two dataframes
merged_inner = pd.merge(left=user_id_purchase_df, right=user_id_signup_df, left_on='user.id', right_on='user.id')

# change date type to datetime
df_purchase_date = pd.to_datetime(merged_inner["purchase.date"])
df_signup_date = pd.to_datetime(merged_inner["signup.date"])

# Calculate diff between purchase_date and signup_date
merged_inner["diff"]=df_purchase_date-df_signup_date

# Set condition values
delta_90 = pd.Timedelta(90, units="days")
delta_0 = pd.Timedelta(0,units="days")

#print(merged_inner)
user_purchase_in_90days= merged_inner.loc[(merged_inner["diff"].dt.days > 0) & (merged_inner["diff"].dt.days <= 90)]
print(user_purchase_in_90days)

# users who purchased in 90 days 
len(user_purchase_in_90days["user.id"].unique()) 
# 6369 who purchased in 90 days, result match with SQL result, total user 23841

# purchased_date and signup_date for users who purchased in 90 days
first_purchase_date = user_purchase_in_90days[["user.id","purchase.date","signup.date"]]
#print(first_purchase_date)

# Group by user_id, Aggregate purchased_date and signup_date
group_by_user_id = first_purchase_date.groupby("user.id").agg("min")
#print(group_by_user_id)

# Merge messages table and grouped users table
merged_mess_pur = pd.merge(left = group_by_user_id, right = df_mes, left_on = 'user.id', right_on = 'user.id')
#print(merged_mess_pur)

# users who purchased in 90 days got messages 
user_got_message = merged_mess_pur.loc[(merged_mess_pur['message.date']>merged_mess_pur['signup.date']) & (merged_mess_pur['message.date']<merged_mess_pur['purchase.date'])]
len(user_got_message["user.id"].unique())
# 2653 users who purchased get messages in 90 days. Result match.

# users who didn't purchase in 90 days
len(df_users1["user.id"].unique())-len(user_purchase_in_90days["user.id"].unique()) 
# 17472 users who didn't purchase in first 90 days. Result matach.

df_user_no_purchase = pd.merge(left = df_users1, right = user_purchase_in_90days, left_on = 'user.id', right_on = 'user.id', how="left",indicator=True)

df_user_no_purchase = df_user_no_purchase[df_user_no_purchase['_merge']=='left_only']
user_no_purchase = df_user_no_purchase[["user.id","signup.date_x"]]
print(user_no_purchase)

# join users table and message teable
df_user_no_purchase_mess= pd.merge(left = user_no_purchase, right = df_mes, left_on = 'user.id', right_on = 'user.id')

message_date = pd.to_datetime(df_user_no_purchase_mess["message.date"])
signup_date = pd.to_datetime(df_user_no_purchase_mess['signup.date_x'])

# no purchased users who got messages in 90 days
nopurchase_got_message = df_user_no_purchase_mess.loc[((message_date -signup_date).dt.days>0) & ((message_date -signup_date).dt.days<=90)]
len(nopurchase_got_message["user.id"].unique())
# 16363 users who didn't purchase but got message in 90 days. Result match

#--------------------------------------------------------------------------------
# Third part ------Creative questions and answers-------------------------------
#--------connect to SQL server---------
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                     'Server=DESKTOP-H5VPCDO\SS17;'
                     'Database=test_db;'
                     'Trusted_Connection=yes;')
cursor = conn.cursor()

# run query and fetch results, show number of users in different month, year
sql="""select concat(t.years,'-',t.months) Times, t.counts 
       from (select top 24 year(signup_date) years, Month(signup_date) Months,count(user_id) counts 
             from dbo.users
             group by year(signup_date),Month(signup_date) 
             order by years, Months)t"""
cursor.execute(sql)
res=cursor.fetchall()

time, user_counts = zip(*res)
# print(user_counts)
# print(time)
x_pos = [i for i,_ in enumerate(time)]

# create plot (Number of users signup in different months)
plt.bar(x_pos, user_counts,color='blue')
plt.xlabel('time')
plt.ylabel('Number of users')
plt.title("Number of users signup in different months")
degrees=90
plt.xticks(x_pos,time)
plt.xticks(rotation=degrees)
plt.show()

# top 20 users who purchases most
sql="""select top 20 user_id, sum(purchase_count) total_purchase
from dbo.purchases p
group by p.user_id
order by total_purchase DESC
"""
cursor.execute(sql)
res=cursor.fetchall()
user_id, purchase_counts = zip(*res)
x_pos = [i for i,_ in enumerate(user_id)]
plt.barh(x_pos,purchase_counts,color='green')
plt.ylabel('user_id')
plt.xlabel('Number of purchase')
plt.title("Top 20 users who bought most")
plt.yticks(x_pos,user_id)
plt.show()

# Top 20 users who got messages most
sql=""";with cte as
(select top 20 user_id, sum(purchase_count) total_purchase
from dbo.purchases p
group by p.user_id
order by total_purchase DESC) 

select top 20 m.user_id, count(*) total_message
from dbo.messages m
group by m.user_id
having user_id in (select user_id from cte)
order by total_message DESC
"""
cursor.execute(sql)
res=cursor.fetchall()
user_id2, message_counts = zip(*res)
x_pos = [i for i,_ in enumerate(user_id2)]
plt.barh(x_pos,message_counts,color='red')
plt.ylabel('user_id')
plt.xlabel('Number of messages')
plt.title("Top 20 users who got messages most")
plt.yticks(x_pos,user_id2)
plt.show()

# print(user_id2)
# print(user_id)

# top 20 users who got the messages most is the users who purchased most
lt=[]
for i in user_id2:
    if i not in user_id:
        lt.append(i)
print(lt)       

# Number of Messages VS Number of Number of product purchased

# Without time limitation
# sql="""
# ;with cte as
# (select top 15000 user_id, sum(purchase_count) total_purchase
# from dbo.purchases p
# group by p.user_id
# order by total_purchase DESC) 

# select count(*) total_message, min(cte.total_purchase) total_purchase
# from dbo.messages m
# Right join cte on cte.user_id = m.user_id
# group by m.user_id
# --having user_id in (select user_id from cte)
# order by total_message DESC, total_purchase DESC
# """

# with 90 days time limitation
sql="""
;with cte as
(select top 15000 p.user_id, sum(purchase_count) total_purchase
from dbo.purchases p
inner join dbo.users u on p.user_id = u.user_id
where datediff(d,u.signup_date,p.purchase_date)<=90 and u.signup_date != p.purchase_date
group by p.user_id
order by total_purchase DESC) 

select count(*) total_message, min(cte.total_purchase) total_purchase
from dbo.messages m
inner join dbo.users u on m.user_id = u.user_id
Right join cte on cte.user_id = m.user_id
where datediff(d,u.signup_date,m.message_date)<=90 and u.signup_date != m.message_date
group by m.user_id
order by total_purchase DESC
"""

cursor.execute(sql)
res=cursor.fetchall()
total_message,total_purchase= zip(*res)
x_pos = [i for i,_ in enumerate(total_message)]

# Create combo chart
fig, ax1 = plt.subplots(figsize=(20,10))
#bar plot creation
ax1.set_xlabel('users', fontsize=16)
ax1.set_ylabel('Number of Messages', fontsize=16)
ax1 = sns.barplot(x=x_pos, y=total_message, color='green')
ax1.tick_params(axis='y')
#specify we want to share the same x-axis
ax2 = ax1.twinx()
#line plot creation
ax2.set_ylabel('Number of Purchase', fontsize=16)
ax2 = sns.lineplot(x=x_pos, y= total_purchase, color='red')
ax2.tick_params(axis='y')
#show plot
plt.show()



