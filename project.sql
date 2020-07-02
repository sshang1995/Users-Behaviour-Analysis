--select top 10 * from dbo.purchases
--select top 10 * from dbo.messages
--select top 10 * from dbo.users

----question 1 ------6369 purchased ,23841 total, 27%
select count(distinct p.user_id) user_puchase_in_90_days,(select count(distinct user_Id) from dbo.users) total_users,round(count(distinct p.user_id)*1.0/(select count(distinct user_Id) from dbo.users),2)*100 percents
from dbo.purchases p
inner join dbo.users u on p.user_id = u.user_id
where Datediff(d,u.signup_date,p.purchase_date)<=90 and datediff(d,signup_date,purchase_date)>0

-------question 2 ---2653 get message, 6369 purchased , 42%
;with user_puchase_in_90_days as (
select p.user_id, min(signup_date) signup_date, Min(p.purchase_date) first_purchase_date
from dbo.purchases p
inner join dbo.users u on p.user_id = u.user_id
where Datediff(d,u.signup_date,p.purchase_date)<=90 and datediff(d,signup_date,purchase_date)>0
group by p.user_id)


select count(user_id) user_recevied_message,(select count(*) from user_puchase_in_90_days) num_of_user_puchase_in_90_days, round(count(*)*1.0/(select count(*) from user_puchase_in_90_days),2)*100 percentages 
from 
(select cte.user_id, signup_date, first_purchase_date, min(m.message_date) first_message_date
from user_puchase_in_90_days cte 
left join dbo.messages m on m.user_id =cte.user_id 
where m.message_date > cte.signup_date and m.message_date < cte.first_purchase_date
group by cte.user_id, signup_date, first_purchase_date) t

-----question 3 --17472 no purchase, 16363 received message, 94%
; with cte as 
(select u.user_id, u.signup_date
from dbo.users u
where u.user_id not in
(select distinct p.user_id
from dbo.purchases p
inner join dbo.users u on p.user_id = u.user_id
where Datediff(d,u.signup_date,p.purchase_date)<=90 and datediff(d,u.signup_date,p.purchase_date)>0)
)

select count(*) user_no_purchase_received_message, (select count(*) from cte) user_no_purchase, round(count(*)*1.00/(select count(*) from cte),2)*100 percentages
from
(select cte.user_id, cte.signup_date
from cte 
left join dbo.messages m  on cte.user_id =m.user_id
where datediff(d,cte.signup_date,m.message_date)<=90 and datediff(d,cte.signup_date,m.message_date)>0
group by cte.user_id, cte.signup_date)t