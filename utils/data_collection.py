# Import the bultin libraries.
import logging
import random
import string
import sys
import time

# Import the third-party libraries.
from bs4 import BeautifulSoup
import pandas as pd
import requests


# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "{asctime} | {name} | {levelname} | {message}", style="{"
)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


########################################################################
# https://www.balldontlie.io/
def create_tables(cur):
    """ Creates creates in the nba database in PostgreSQL.

    Creates the 4 tables in the nba database:
        - teams,
        - games,
        - players,
        - stats.
    The column names are based on the JSON-file received over the API.
    """
    # Create the teams table.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
               id           INTEGER PRIMARY KEY,
               abbreviation TEXT,
               city         TEXT,
               conference   TEXT,
               division     TEXT,
               full_name    TEXT,
               name         TEXT
        )
    """)
    # Create the games table.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
               id                 INTEGER    PRIMARY KEY,
               date               TIMESTAMP,
               home_team_id       INTEGER,
               home_team_score    INTEGER,
               season             INTEGER,
               visitor_team_id    INTEGER,
               visitor_team_score INTEGER,
               FOREIGN KEY (home_team_id) REFERENCES teams (id),
               FOREIGN KEY (visitor_team_id) REFERENCES teams (id)
        )   
    """)
    # Create the players table.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
               id         INTEGER PRIMARY KEY,
               first_name TEXT,
               last_name  TEXT,           
               position   TEXT,
               team_id    INTEGER,
               FOREIGN KEY (team_id) REFERENCES teams (id)
        )
    """)
    # Create the stats table.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats (
               id        INTEGER PRIMARY KEY,
               game_id   INTEGER,
               team_id   INTEGER,
               player_id INTEGER,
               ast       INTEGER,
               blk       INTEGER,
               dreb      INTEGER,
               fg3_pct   NUMERIC,
               fg3a      INTEGER,
               fg3m      INTEGER,
               fg_pct    NUMERIC,
               fga       INTEGER,
               fgm       INTEGER,
               ft_pct    NUMERIC,
               fta       INTEGER,
               ftm       INTEGER,
               min       TEXT,
               oreb      INTEGER,
               pf        INTEGER,
               pts       INTEGER,
               reb       INTEGER,
               stl       INTEGER,
               turnover  INTEGER,
               FOREIGN KEY (game_id) REFERENCES games (id),
               FOREIGN KEY (team_id) REFERENCES teams (id),
               FOREIGN KEY (player_id) REFERENCES players (id)
        )
    """)


def get_table_names(cur):
    """Gets the table names in the nba database."""
    # Execute the query to get all table names.
    cur.execute("""
        SELECT table_name 
          FROM information_schema.tables
         WHERE table_schema = 'public'    
    """)
    # cur.execute("""
    #     SELECT tablename
    #       FROM pg_tables
    #      WHERE schemaname='public'
    # """)
    # Fetch and print the table names.
    table_names = cur.fetchall()
    tab_names = [table_name[0] for table_name in table_names]
    logger.info(f"The following tables have been created: {tab_names}.")


def insert_data_to_teams(cur, conn):
    """Retrieves and inserts data from API to the teams table.

    The function retrieves, inserts the data and save changes in the DB.
    A number of teams is small, and we want to avoid violation of key
    constraints. Therefore, populate all the table.
    """
    # Retrieve the teams data.
    try:
        response = requests.get(
            url="https://www.balldontlie.io/api/v1/teams",
            params={"per_page": 100}
        )
        response.raise_for_status()
        logger.info(f"Successful API call to the teams endpoint.")
        teams_data = response.json()["data"]
    except requests.RequestException as e:
        logger.error(f"API requests failed: {e}.")
        raise SystemExit("API request failed. Exiting program.")
    teams = []
    for team in teams_data:
        teams.append({
            "id": team["id"],
            "abbreviation": team["abbreviation"],
            "city": team["city"] or None,
            "conference": team["conference"].strip() or None,
            "division": team["division"] or None,
            "full_name": team["full_name"],
            "name": team["name"]
        })
    # Insert the data from dicts to the teams table.
    teams_query = ("""
        INSERT INTO teams (
               id, 
               abbreviation, 
               city, 
               conference, 
               division, 
               full_name, 
               name
        )
        VALUES (
               %(id)s,
               %(abbreviation)s,
               %(city)s,
               %(conference)s,
               %(division)s,
               %(full_name)s,
               %(name)s
        )
        ON CONFLICT (id) DO NOTHING
    """)
    cur.executemany(teams_query, teams)
    logger.info("The data have been inserted to the teams tables.")
    conn.commit()
    logger.info("The changes in the DB have been saved.")


def get_data(cur, conn, seasons, start_page=1):
    """Gets data over API and insert it to the DB.

    This function:
        - Gets data over API.
        - Create the batches.
        - Call the insert_data function to insert the batches.
        - Save changes in the DB after every batch insertion.
    """
    # Set the API endpoint URL
    url = "https://www.balldontlie.io/api/v1/stats"
    # Set the initial page number and per_page parameters.
    page = start_page
    per_page = 100
    # Create empty lists to store the data for every table.
    games_buffer = []
    players_buffer = []
    stats_buffer = []
    # Create a counter for the number of pages retrieved in the
    # current batch.
    pages_in_batch = 0
    while True:
        params = {"page": page, "per_page": per_page, "seasons[]": seasons}
        try:
            # Send an HTTP GET request to the API endpoint and check
            # the status.
            response = requests.get(url, params=params)
            response.raise_for_status()
            logger.debug(f"Successful API call to the stats endpoint, "
                         f"request page {page} for season {seasons}.")
            # Get json/dict from the response.
            data = response.json()
            # logger.debug(f"{data}")
        except requests.RequestException as e:
            logger.error(f"API requests failed: {e}.")
            raise SystemExit("API request failed. Exiting program.")
        # Create a list of dicts for every table.
        for item in data["data"]:
            games_buffer.append(item["game"])
            # Use team_id from "team" rather than "player" because
            # some players played for several teams in one season. So,
            # we use the current team. Also, we use "if" because the
            # value of some item["player"] is None.
            if item["player"]:
                players_buffer.append(
                    {
                        "id": item["player"]["id"],
                        "first_name": item["player"]["first_name"],
                        "last_name": item["player"]["last_name"],
                        "position": item["player"]["position"] or None,
                        "team_id": item["team"]["id"]
                    }
                )
                # Describe every key because we exclude team, game,
                # and player and add the foreign keys for these columns.
                stats_buffer.append(
                    {
                        "id": item["id"],
                        "game_id": item["game"]["id"],
                        "team_id": item["team"]["id"],
                        "player_id": item["player"]["id"],
                        "ast": item["ast"],
                        "blk": item["blk"],
                        "dreb": item["dreb"],
                        "fg3_pct": item["fg3_pct"],
                        "fg3a": item["fg3a"],
                        "fg3m": item["fg3m"],
                        "fg_pct": item["fg_pct"],
                        "fga": item["fga"],
                        "fgm": item["fgm"],
                        "ft_pct": item["ft_pct"],
                        "fta": item["fta"],
                        "ftm": item["ftm"],
                        "min": item["min"],
                        "oreb": item["oreb"],
                        "pf": item["pf"],
                        "pts": item["pts"],
                        "reb": item["reb"],
                        "stl": item["stl"],
                        "turnover": item["turnover"]
                    }
                )
        pages_in_batch += 1
        # If 10 pages retrieved or we're on the last page, insert the
        # data and reset buffers.
        if pages_in_batch == 50 or page == data["meta"]["total_pages"]:
            # Insert a batch.
            insert_data(cur, (games_buffer, players_buffer, stats_buffer))
            logger.info(
                f"The data (Season {seasons}: {page - pages_in_batch + 1}"
                f"-{page} pages) have been inserted into the games, players, "
                f"and stats tables."
            )
            # Save changes in the DB.
            conn.commit()
            logger.info(
                f"The changes in the games, players and stats tables have been"
                f" saved (Season {seasons}: {page - pages_in_batch + 1}-"
                f"{page} pages)."
            )
            # Reset the buffers and counter.
            games_buffer = []
            players_buffer = []
            stats_buffer = []
            pages_in_batch = 0
            logger.debug("The table buffers have been cleared.")
        # Check the page number and use 1 sec delay (not more than
        # 60 API requests per minute.)
        if data["meta"]["current_page"] < data["meta"]["total_pages"]:
            page += 1
            # time.sleep(1)
            # logger.debug(f'Current page: {data["meta"]["current_page"]}')
            # logger.debug(f'Total pages: {data["meta"]["total_pages"]}')
        else:
            logger.info(
                f"The API requests have been completed. "
                f"The last page for Season {seasons}: {page}."
            )
            break


def insert_data(cur, buffers):
    """Inserts data received over API to the nba database."""
    games_query = ("""
        INSERT INTO games (
               id,
               date,
               home_team_id,
               home_team_score,
               season,
               visitor_team_id,
               visitor_team_score               
        )
        VALUES (
               %(id)s,
               %(date)s,
               %(home_team_id)s,
               %(home_team_score)s,
               %(season)s,
               %(visitor_team_id)s,
               %(visitor_team_score)s
        )
        ON CONFLICT (id) DO NOTHING
    """)
    cur.executemany(games_query, buffers[0])
    # Insert the data from dicts to the players table.
    players_query = ("""
        INSERT INTO players (
               id,
               first_name,
               last_name,
               position,
               team_id
        )
        VALUES (
               %(id)s,
               %(first_name)s,
               %(last_name)s,
               %(position)s,
               %(team_id)s
        )
        ON CONFLICT (id) DO NOTHING
    """)
    cur.executemany(players_query, buffers[1])
    # Insert the data from dicts to the stats table.
    stats_query = ("""
        INSERT INTO stats (
               id,
               game_id,
               team_id,
               player_id,
               ast,
               blk,
               dreb,
               fg3_pct,
               fg3a,
               fg3m,
               fg_pct,
               fga,
               fgm,
               ft_pct,
               fta,
               ftm,
               min,
               oreb,
               pf,
               pts,
               reb,
               stl,
               turnover
        )
        VALUES (
               %(id)s,
               %(game_id)s,
               %(team_id)s,
               %(player_id)s,
               %(ast)s,
               %(blk)s,
               %(dreb)s,
               %(fg3_pct)s,
               %(fg3a)s,
               %(fg3m)s,
               %(fg_pct)s,
               %(fga)s,
               %(fgm)s,
               %(ft_pct)s,
               %(fta)s,
               %(ftm)s,
               %(min)s,
               %(oreb)s,
               %(pf)s,
               %(pts)s,
               %(reb)s,
               %(stl)s,
               %(turnover)s
        )
        ON CONFLICT (id) DO NOTHING
    """)
    cur.executemany(stats_query, buffers[2])


########################################################################
# https://www.basketball-reference.com/
def fetch_basketball_data(letter):
    """Fetches basketball player data.

     Fetches basketball player data based on the initial letter of
     their surname.
     """
    # Format the URL to get data for players with surnames starting
    # with the given letter.
    url = f"https://www.basketball-reference.com/players/{letter}"
    response = requests.get(url)
    # Check if the request was successful.
    if response.status_code != 200:
        logger.warning(f"Failed to fetch data for letter: {letter}.")
        return None
    # Parse the HTML response content using BeautifulSoup.
    soup = BeautifulSoup(response.content, "html.parser")
    # Find the main table on the page.
    table = soup.find("table")
    # Fetch all the rows in the table skipping the header.
    rows = table.find_all("tr")[1:]
    # Create a dictionary to store extracted data. Every element of
    # the dict is a list.
    data = {
        "name": [],
        "from_year": [],
        "to_year": [],
        "pos": [],
        "height": [],
        "weight": [],
        "birth_date": [],
        "college": []
    }
    # Iterate over rows to extract player details.
    for row in rows:
        data["name"].append(row.find("th", {"data-stat": "player"}).text)
        data["from_year"].append(
            row.find("td", {"data-stat": "year_min"}).text
        )
        data["to_year"].append(row.find("td", {"data-stat": "year_max"}).text)
        data["pos"].append(row.find("td", {"data-stat": "pos"}).text)
        data["height"].append(row.find("td", {"data-stat": "height"}).text)
        data["weight"].append(row.find("td", {"data-stat": "weight"}).text)
        data["birth_date"].append(
            row.find("td", {"data-stat": "birth_date"}).text
        )
        data["college"].append(row.find("td", {"data-stat": "colleges"}).text)
    # Convert the dictionary to a Pandas DataFrame.
    df = pd.DataFrame(data)
    # Set random delays.
    sleep_time = random.randint(60, 120)
    time.sleep(sleep_time)
    logger.info(
        f"Fetch data for letter: {letter}. Waiting for {sleep_time} s ..."
    )
    return df


def fetch_all_data():
    """Fetches basketball player data for all letters."""
    # List to store DataFrames for each individual letter.
    df_letters = []
    # Iterate over each letter of the alphabet.
    for letter in string.ascii_lowercase:
        # Fetch player data for the given letter.
        df_letter = fetch_basketball_data(letter)
        if df_letter is not None:
            df_letters.append(df_letter)
    # Combine all the individual DataFrames into one.
    result_df = pd.concat(df_letters, ignore_index=True)
    return result_df


def prepare_df(df):
    """Does transformations with df."""
    df['birth_date'] = pd.to_datetime(df['birth_date'])
    return df
