select * from event
select * from email

-- new user signup funnel analysis
with user_funnel as(
select event_type, event_name, count(distinct user_id) as n_users
from event
where event_type = 'signup_flow' and occured_at >= '2014-06-01' and occured_at <= '2014-09-01'
group by 1, 2
order by 3 desc
)

select event_type, event_name, n_users, max(n_users) over (partition by null order by null) as total_users
, round(n_users*1.0/max(n_users) over (partition by null order by null), 2) as conversion_rate
from user_funnel

-- general trend
select date_trunc('day', occured_at)::date as week
, count(case when event_name = 'create_user' then user_id else null end) as create_users
, count(case when event_name = 'enter_email' then user_id else null end) as enter_email_users
, count(case when event_name = 'enter_info' then user_id else null end) as enter_info_users
, count(case when event_name = 'complete_signup' then user_id else null end) as complete_signup_users
from event
where date_trunc('day', occured_at)::date >= '2014-06-01' and date_trunc('day', occured_at)::date <= '2014-09-01'
group by 1
order by 1

-- cohort analysis
with age as (
select u.user_id, 
u.event_name as signup, u.occured_at as signup_date,
e.event_name as login, e.occured_at as login_date,
date_part('day',e.occured_at - u.occured_at) as user_age_event,
date_part('day',('2014-09-01'::timestamp) - u.occured_at) as user_age
from event u
join event e
on u.user_id = e.user_id
and u.event_type = 'signup_flow' and u.event_name = 'complete_signup'
and e.event_type = 'engagement' and e.event_name = 'login'
)

select date_trunc('week', login_date)::date as week,
count(case when user_age >77 then user_id else null end) as "11+ weeks",
count(case when user_age <77 and user_age >= 70 then user_id else null end) as "10 weeks",
count(case when user_age <70 and user_age >= 63 then user_id else null end) as "9 weeks",
count(case when user_age <63 and user_age >= 56 then user_id else null end) as "8 weeks",
count(case when user_age <56 and user_age >= 49 then user_id else null end) as "7 weeks",
count(case when user_age <49 and user_age >= 42 then user_id else null end) as "6 weeks",
count(case when user_age <42 and user_age >= 35 then user_id else null end) as "5 weeks",
count(case when user_age <35 and user_age >= 28 then user_id else null end) as "4 weeks",
count(case when user_age <28 and user_age >= 21 then user_id else null end) as "3 weeks",
count(case when user_age <21 and user_age >= 14 then user_id else null end) as "2 weeks",
count(case when user_age <14 and user_age >= 7 then user_id else null end) as "1 weeks",
count(case when user_age <7 then user_id else null end) as "less than 1 week"
from age
group by 1

-- retention analysis
with age as (
select u.user_id, 
u.event_name as signup, u.occured_at as signup_date,
e.event_name as login, e.occured_at as login_date,
date_part('day',e.occured_at - u.occured_at) as user_age_event,
date_part('day',('2014-09-01'::timestamp) - u.occured_at) as user_age
from event u
join event e
on u.user_id = e.user_id
and u.event_type = 'signup_flow' and u.event_name = 'complete_signup'
and e.event_type = 'engagement' and e.event_name = 'login'
),

active as (
select 
date_trunc ('week', signup_date) as signup_week,
date_trunc ('week', login_date) as login_week,
extract('day' from (date_trunc ('week', login_date)-date_trunc ('week', signup_date)))/7 as dt,
count(distinct user_id) as user_cnt
from age
group by 1, 2, 3
order by 1, 2, 4
)

select signup_week, login_week, dt, user_cnt,
first_value(user_cnt) over (partition by signup_week order by login_week asc) as user_base,
round(user_cnt * 1.0/first_value(user_cnt) over (partition by signup_week order by login_week asc),2) as retention_rate
from active

-- device analysis
select date_trunc('week', e.occured_at)::date as week,
count(distinct e.user_id) as weekly_active_users,
count(distinct case when e.device in ('macbook pro', 'lenovo thinkpad', 'macbook air', 'dell inspiron notebook', 'dell inspiron desktop', 'asus chromebook', 'acer aspire notebook', 'hp pavilion desktop', 'acer aspire desktop', 'mac mini') then e.user_id else null end) as computer,
count(distinct case when e.device in ('iphone 5', 'samsung galaxy s4', 'nexus 5', 'iphone 5s', 'iphone 4s', 'nokia lumia 635', 'htc one', 'samsung galaxy note', 'amazon fire phone') then e.user_id else null end) as phone,
count(distinct case when e.device in ('ipad air', 'nexus 7', 'ipad mini', 'nexus 10', 'kindle fire', 'windows surface', 'samsung galaxy tablet') then e.user_id else null end) as tablet
from event e
where e.event_type = 'engagement'
and e.event_name = 'login'
and e.occured_at >= '2014-06-01'
and e.occured_at < '2014-09-01'
group by 1
order by 1

-- email analysis
with email_funnel as (
select date_trunc('week', date(occured_at)) as week,
count(case when action_type = 'sent_weekly_digest' then user_id else null end) as weekly_digest,
count(case when action_type = 'sent_reengagement_email' then user_id else null end) as reengagement_emails,
count(case when action_type = 'email_open' then user_id else null end) as email_opens,
count(case when action_type = 'email_clickthrough' then user_id else null end) as email_clickthroughs,
count(case when action_type in ('sent_weekly_digest', 'sent_reengagement_email') then user_id else null end) as email_sent
from email
group by 1
order by 1
)

select week, email_sent, weekly_digest, reengagement_emails, email_opens, email_clickthroughs, 
email_clickthroughs*1.0/email_sent as email_ctr
from email_funnel





