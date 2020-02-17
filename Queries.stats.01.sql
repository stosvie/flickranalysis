select statdate, 'frozen' as stats_state into dbo.stats_status
FROM (
select DISTINCT statdate from dbo.photo_stats)Q

select * from dbo.stats_status
select cast(getdate() as date) as  today, (select max(statdate) from dbo.stats_status where stats_state = 'frozen')from dbo.stats_status 
select max(statdate) ,stats_state from dbo.stats_status 
group by stats_state
union 
select  cast(getdate() as date) as  statdate, 'today' as stats_state

select max(statdate)  as stats_date,stats_state from dbo.stats_status group by stats_state 
--union select  '2020-02-14' as  statdate, 'live' as stats_state
union select  cast(getdate() as date) as  statdate, 'today' as stats_state

--delete from dbo.photo_stats where statdate = '2020-02-14'



select distinct statdate  from dbo.photo_stats_domains
--truncate table  dbo.photo_stats_domains
select count(*), sum(cast(views as int)), cat from (
select case 
            when [url] like '%/groups/%' then 'groups' 
            when [url] like '%galleries/%' then 'galeries' 
            when [url] like '%favorites/%' then 'favs' 
            when [url] like '%/photos/tags/%' then 'tags' 
            
            when [url] like '%/photos%' then 'photos' 
            when [url] like '%/search%' then 'search' ELSE  
                'other' END as cat, *
                 from dbo.photo_stats_domains
)Q group by cat

order by url


select * from dbo.photo_stats_domains where url like '%and%'

select count(*), , domain from dbo.photo_stats_domains
group by domain

order by 1

select --avg(favorites)
select sum(favorites) ,statsdate
* from dbo.photo_stats where (statdate  between '2020-02-13' and  '2020-02-16' ) and  favorites <> 0

select round(avg(favorites), 0) FROM (
select sum(favorites) as favorites,statdate
 from dbo.photo_stats 
where favorites <> 0
group by statdate
) Q where statdate  between '2020-2-1' and  '2020-02-16'


select round(avg(views), 0) FROM (
select sum(views) as views,statdate
 from dbo.photo_stats 
where views <> 0
group by statdate
) Q where statdate  between '2020-2-1' and  '2020-02-16'





order by statdate




