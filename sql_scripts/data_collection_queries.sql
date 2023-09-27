-- Drop the tables.
/*
DROP TABLE IF EXISTS games CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS stats;
*/

-- Select the data from all created tables.
SELECT *
  FROM teams;
 
SELECT *
  FROM games;
 
SELECT *
  FROM players;
 
SELECT *
  FROM stats;
 

-- Check a few playoff games in 2019. 
SELECT *
  FROM games AS g
  JOIN teams AS th
    ON g.home_team_id = th.id
  JOIN teams AS tv
    ON g.visitor_team_id = tv.id
 WHERE date BETWEEN '2019-04-13' AND '2019-06-16'
 ORDER BY date; 


-- Count total rows in stats.
SELECT COUNT(*)
  FROM stats;
 
 
-- Check that all seasons(years) are included. 
SELECT EXTRACT(YEAR FROM date) AS "Year",
	   COUNT(*) AS "Amount of games"
  FROM games 
 GROUP BY EXTRACT(YEAR FROM date)
 ORDER BY "Year";

--
SELECT *
  FROM stats
  JOIN games
    ON stats.game_id = games.id
 WHERE EXTRACT(YEAR FROM date) = '2020'
   AND player_id = 666523; 

  
 SELECT *
   FROM players;

 SELECT *
   FROM stats
   JOIN players
     ON stats.player_id = players.id
  WHERE EXTRACT(YEAR FROM date) = '2020'; 
  
SELECT *
  FROM stats AS s
  JOIN games AS g
    ON s.game_id = g.id
  JOIN players AS p
    ON s.player_id = p.id
  JOIN teams AS t
    ON s.team_id = t.id
 WHERE EXTRACT(YEAR FROM date) BETWEEN '1983' AND '2023';   


SELECT s.player_id,
	   p.first_name,
	   p.last_name,
	   t.abbreviation,
	   t.full_name,
	   p.position,
   	   g.season,
	   s.ast,
	   s.blk,
	   s.dreb,
	   s.fg3_pct,
	   s.fg3a,
	   s.fg3m,
	   s.fg_pct,
	   s.fga,
	   s.fgm,
	   s.ft_pct,
	   s.fta,
	   s.ftm,
	   s.min,
	   s.oreb,
	   s.pf,
	   s.pts,
	   s.reb,
	   s.stl,
	   s.turnover,
	   s.team_id,
	   s.game_id,
	   g.date
  FROM stats AS s
  JOIN games AS g
    ON s.game_id = g.id
  JOIN players AS p
    ON s.player_id = p.id
  JOIN teams AS t
    ON s.team_id = t.id
 WHERE g.season BETWEEN '1983' AND '2023';  

  WITH player_career AS (
SELECT s.player_id,
	   EXTRACT (YEAR FROM MIN(date)) AS from_year,
	   EXTRACT (YEAR FROM MAX(date)) AS to_year
  FROM stats AS s
  JOIN games AS g
    ON s.game_id = g.id
  JOIN players AS p
    ON s.player_id = p.id
  JOIN teams AS t
    ON s.team_id = t.id
 GROUP BY s.player_id, p.first_name, p.last_name)
SELECT s.player_id,
	   p.first_name,
	   p.last_name,
	   t.abbreviation,
	   t.full_name,
	   p.position,
   	   g.season,
	   s.ast,
	   s.blk,
	   s.dreb,
	   s.fg3_pct,
	   s.fg3a,
	   s.fg3m,
	   s.fg_pct,
	   s.fga,
	   s.fgm,
	   s.ft_pct,
	   s.fta,
	   s.ftm,
	   s.min,
	   s.oreb,
	   s.pf,
	   s.pts,
	   s.reb,
	   s.stl,
	   s.turnover,
	   s.team_id,
	   s.game_id,
	   g.date,
	   pc.from_year,
	   pc.to_year
  FROM stats AS s
  JOIN games AS g
    ON s.game_id = g.id
  JOIN players AS p
    ON s.player_id = p.id
  JOIN teams AS t
    ON s.team_id = t.id
  JOIN player_career AS pc
    ON s.player_id = pc.player_id
 WHERE g.season BETWEEN '1983' AND '2023'; 




SELECT *
  FROM stats AS s
  JOIN games AS g
    ON s.game_id = g.id
  JOIN players AS p
    ON s.player_id = p.id
  JOIN teams AS t
    ON s.team_id = t.id
 WHERE g.season BETWEEN '1983' AND '2023'
   AND p.first_name = 'Robert'
   AND p.last_name = 'Werdann';  


SELECT * 
  FROM players
 WHERE first_name = 'Jeff'
   AND last_name = 'Taylor';

--
SELECT s.player_id,
	   p.first_name,
	   p.last_name,
	   COUNT(*)
  FROM stats AS s
  JOIN games AS g
    ON s.game_id = g.id
  JOIN players AS p
    ON s.player_id = p.id
  JOIN teams AS t
    ON s.team_id = t.id
 WHERE g.season BETWEEN '1983' AND '2023'
 GROUP BY player_id, first_name, last_name;  

 
 
-- Check some details. 
SELECT city
  FROM teams
 WHERE abbreviation = 'CHS'; 
 
SELECT *
  FROM teams
 WHERE city IS NOT NULL; 
 
SELECT *
  FROM teams
 WHERE city != ''; 
 

SELECT DISTINCT(player_id) 
  FROM stats;

SELECT COUNT(*)
  FROM stats;
  
 
SELECT *
  FROM games
 WHERE date BETWEEN '2019-04-13' AND '2019-06-16'; 
 


