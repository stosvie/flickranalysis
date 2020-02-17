SELECT TOP (1000) [index]
      ,[id]
      ,[title]
      ,[comments]
      ,[favorites]
      ,[total_comments]
      ,[total_favorites]
      ,[total_views]
      ,[views]
      ,[statdate]

  select id,  SUM(favorites), sum(views),sum(comments)
  FROM [dbo].[photo_stats]
  where statdate = '2020-02-05'
  group by id
  order by 3 desc



select  t.photo_id, sum(q.v), sum(q.f)
from photo_tags t 
left join (
      select id,  SUM(favorites) f, sum(views) v,sum(comments) c
  FROM [dbo].[photo_stats]
  group by id
  --order by 3 desc

)Q on q.id = t.photo_id
where raw like '%drone%'
group by t.photo_id
order by 3 desc

select min (statdate) from dbo.photo_stats












  select * from  [dbo].[photo_stats]
  where id = 49391703112
  order by STATdate



