-- TODO replace with correct projection columns
-- Ansower

select team_name, full_name, points
from 
(
    select 
		team_name
		,full_name
		,points
		,rank() OVER (PARTITION BY team_name order by points desc) as rank
	from {{ ref('nhl_players') }}
	where points > 0 
) as base
where rank = 1