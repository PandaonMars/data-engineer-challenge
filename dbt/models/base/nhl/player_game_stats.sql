select
        player_person_id as nhl_player_id,
        side,
        player_person_fullName full_name,
        player_person_link link,
        player_person_firstName first_name,
        player_person_lastName last_name,
        player_person_primaryNumber primary_number,
        player_person_birthDate birth_date,
        player_person_currentAge age_at_gametime,
        player_person_birthCity birth_city,
        player_person_birthStateProvince birth_state_province,
        player_person_birthCountry birth_country,
        player_person_nationality nationality,
        player_person_height height,
        player_person_weight weight,
        player_person_active active,
        player_person_alternateCaptain was_alternative_captain,
        player_person_captain was_captain,
        player_person_rookie was_rookie,
        player_person_shootsCatches shoots_catches,
        player_person_rosterStatus roster_status,
        player_person_currentTeam_id game_team_id,
        player_person_currentTeam_name game_team_name,
        player_person_currentTeam_link game_team_link,
        player_person_primaryPosition_code primary_position_code,
        player_person_primaryPosition_name primary_position_name,
        player_person_primaryPosition_type primary_position_type,
        player_person_primaryPosition_abbreviation primary_position_abbreviation,
        player_jerseyNumber jersey_number,
        player_position_code position_code,
        player_position_name position_name,
        player_position_type position_type,
        player_position_abbreviation position_abbreviation,
        player_stats_skaterStats_timeOnIce stats_time_on_ice,
        player_stats_skaterStats_assists stats_assists,
        player_stats_skaterStats_goals stats_goals,
        player_stats_skaterStats_shots stats_shots,
        player_stats_skaterStats_hits stats_hits,
        player_stats_skaterStats_powerPlayGoals stats_power_play_goals,
        player_stats_skaterStats_powerPlayAssists stats_power_play_assists,
        player_stats_skaterStats_penaltyMinutes stats_penalty_minutes,
        player_stats_skaterStats_faceOffPct stats_face_off_pct,
        player_stats_skaterStats_faceOffWins stats_face_off_wins,
        player_stats_skaterStats_faceoffTaken stats_face_off_taken,
        player_stats_skaterStats_takeaways stats_takeaways,
        player_stats_skaterStats_giveaways stats_giveaways,
        player_stats_skaterStats_shortHandedGoals stats_shorthanded_goals,
        player_stats_skaterStats_shortHandedAssists stats_shorthanded_assists, 
        player_stats_skaterStats_blocked stats_blocked,
        player_stats_skaterStats_plusMinus stats_plus_minus,
        player_stats_skaterStats_evenTimeOnIce stats_event_time_on_ice,
        player_stats_skaterStats_powerPlayTimeOnIce stats_power_play_time_on_ice,
        player_stats_skaterStats_shortHandedTimeOnIce stats_shorthanded_time_on_ice,
        player_stats_goalieStats_timeOnIce goalie_stats_time_on_ice,
        player_stats_goalieStats_assists goalie_stats_assists,
        player_stats_goalieStats_goals goalie_stats_goals,
        player_stats_goalieStats_pim goalie_stats_pim,
        player_stats_goalieStats_shots goalie_stats_shots,
        player_stats_goalieStats_saves goalie_stats_saves,
        player_stats_goalieStats_powerPlaySaves goalie_stats_power_play_saves,
        player_stats_goalieStats_shortHandedSaves goalie_stats_short_handed_saves,
        player_stats_goalieStats_evenSaves goalie_stats_even_saves, 
        player_stats_goalieStats_shortHandedShotsAgainst goalie_stats_shorthanded_shots_against,
        player_stats_goalieStats_evenShotsAgainst goalie_stats_even_shots_against,
        player_stats_goalieStats_powerPlayShotsAgainst goalie_stats_power_play_shots_against,
        player_stats_goalieStats_decision goalie_stats_decision,
        player_stats_goalieStats_savePercentage goalie_stats_save_percentage,
        player_stats_goalieStats_powerPlaySavePercentage goalie_stats_power_play_save_percentage,
        player_stats_goalieStats_evenStrengthSavePercentage goalie_stats_even_strenght_save_percentage,
        player_stats_goalieStats_shortHandedSavePercentage goalie_stats_shorthanded_save_percentage
  from {{ source('nhl', 'game_stats') }}

