

	 SELECT CAST(dt as DATE) as [stats_date] 
	,CASE WHEN s.[stats_date] IS NULL THEN  'unsynced' else s.stats_state END  AS  stats_state
    from 
	  (	SELECT  CAST(DATEADD(DAY, nbr - 1, DATEADD(DAY,-28, GETDATE())) AS DATE) as dt
				FROM    ( SELECT    ROW_NUMBER() OVER ( ORDER BY c.object_id ) AS Nbr
				FROM      sys.columns c
			) nbrs
		WHERE   nbr - 1 <= DATEDIFF(DAY, DATEADD(DAY,-28, GETDATE()), GETDATE())
		) BASE
		LEFT JOIN fs.stats_status s on base.dt = CAST(s.[stats_date] as DATE)
	

		ORDER BY 1 



