-- Answer
select
nhl_player_id as id
,full_name
,game_team_name team_name
,sum(stats_assists) as assists
,sum(stats_goals) as goals
,sum(stats_assists) + sum(stats_goals) as points
from {{ ref('player_game_stats') }}
group by 1, 2, 3