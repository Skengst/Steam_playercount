#Imports
import requests
import pandas as pd
import duckdb
import os
import hashlib
from datetime import datetime
from dotenv import load_dotenv


#Variables


#Functions

def create_database():
    # Creates or connects to the DuckDB database
    conn = duckdb.connect('steam_database.duckdb')
    
    # Create the game information table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS game_information (
            game_id INTEGER PRIMARY KEY,  -- Unique ID for the game
            name TEXT,                    -- Name of the game
            developer TEXT,               -- Developer of the game
            publisher TEXT,               -- Publisher of the game
            release_date DATE,            -- Release date of the game
            genre_id INTEGER,             -- Foreign key to the game_genres table
            price DECIMAL(6, 2)           -- Price of the game
        )
    """)

    # Create the player count table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playercount (
            id INTEGER,              -- Unique identifier for each entry
            game_id INTEGER,         -- Steam App ID of the game
            hour INTEGER,            -- Hour of the player count record
            date DATE,               -- Date of the player count record
            playercount INTEGER,      -- Player count at the given time
            FOREIGN KEY (game_id) REFERENCES game_information(game_id)     
        )
    """)    

    # Create the game genres table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS game_genres (
            genre_id INTEGER PRIMARY KEY, -- Unique ID for the genre
            game_id INTEGER,              -- Steam App ID of the game
            genre TEXT,                   -- Genre of the game
            FOREIGN KEY (game_id) REFERENCES game_information(game_id)
        )
    """)

    print("Database and tables created successfully.")
    conn.close()


def get_player_count(API_key, appid):
    url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    
    params = {
        "key": API_key,
        "appid": appid
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if "response" in data and "player_count" in data["response"]:
            return data["response"]["player_count"]
        else:
            return "No player count information available."
        
    except requests.exceptions.HTTPError as http_err:
        return f"HTTP error occurred: {http_err}"
    except Exception as err:
        return f"An error occurred: {err}"

def get_game_details(appid):
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 
        data = response.json()
        if str(appid) in data and data[str(appid)]["success"]:
            return data[str(appid)]["data"]
        else:
            return "Game details not found"
    
    except requests.exceptions.HTTPError as http_err:
        return f"HTTP error occurred: {http_err}"
    except Exception as err:
        return f"An error occurred: {err}"


def insert_game_details(conn, appid, game_details):
    if game_details:
        # Check if game already exists in the database
        existing = conn.execute("SELECT COUNT(*) FROM game_information WHERE game_id = ?", (appid,)).fetchone()[0]
        if existing > 0:
            print(f"Game ID {appid} already exists in the database. Skipping insertion.")
            return

        name = game_details.get("name")
        developer = ", ".join(game_details.get("developers", []))
        publisher = ", ".join(game_details.get("publishers", []))
        release_date = game_details.get("release_date", {}).get("date", None)
        if release_date:
            try:
                release_date = datetime.strptime(release_date, "%d %b, %Y").date()
            except ValueError:
                release_date = None  # Handle cases where the format is unexpected
        price = game_details.get("price_overview", {}).get("final", 0) / 100.0  # Convert from cents to euros

        conn.execute("""
            INSERT INTO game_information (game_id, name, developer, publisher, release_date, price)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (appid, name, developer, publisher, release_date, price))

        # Insert genres into game_genres
        for genre in game_details.get("genres", []):
            genre_name = genre["description"]
            
            # Create a deterministic hash-based ID
            unique_string = f"{appid}:{genre_name}"
            genre_id = int(hashlib.sha256(unique_string.encode()).hexdigest(), 16) % (10**9)  # Truncate hash to fit an INTEGER

            conn.execute("""
                INSERT INTO game_genres (genre_id, game_id, genre)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
            """, (genre_id, appid, genre_name))

        print(f"Game details for App ID {appid} inserted successfully.")

def insert_player_count(conn, appid, player_count):
    if player_count is not None:
        now = datetime.now()
        hour = now.hour
        date = now.date()

        conn.execute("""
            INSERT INTO playercount (game_id, hour, date, playercount)
            VALUES (?, ?, ?, ?)
        """, (appid, hour, date, player_count))

        print(f"Player count for App ID {appid} inserted successfully.")

def process_games_from_csv(csv_file, conn):
    appid_list = pd.read_csv(csv_file)["appid"].tolist()
    
    for appid in appid_list:
        try: 
            game_details = get_game_details(appid)
            insert_game_details(conn, appid, game_details)

            player_count = get_player_count(API_key, appid)
            insert_player_count(conn, appid, player_count)
        except Exception as e:
            print(f"Error processing appid {appid}: {e}")


#Start of program
if __name__ == "__main__":
    load_dotenv()
    API_key = os.getenv("API_KEY")

    if not API_key:
        print("API Key not found. Please check your .env file.")
        exit(1)
    
    create_database()

    with duckdb.connect('steam_database.duckdb') as conn:
        process_games_from_csv('game_id.csv', conn)
