#Imports
import requests
import pandas as pd
import duckdb
from datetime import datetime
from dotenv import load_dotenv

#Variables
API_key = 
appid = 1643320


#Functions
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
        price = game_details.get("price_overview", {}).get("final", 0) / 100.0  # Convert from cents to euros

        conn.execute("""
            INSERT INTO game_information (game_id, name, developer, publisher, release_date, price)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (appid, name, developer, publisher, release_date, price))

        # Insert genres into game_genres
        for genre in game_details.get("genres", []):
            genre_name = genre["description"]
            conn.execute("""
                INSERT INTO game_genres (game_id, genre)
                VALUES (?, ?)
            """, (appid, genre_name))

        print(f"Game details for App ID {appid} inserted successfully.")

game_details = get_game_details(appid)
if isinstance(game_details, dict):
    print(f"Game Name: {game_details['name']}")
    print(f"Description: {game_details['short_description']}")
    print(f"Genres: {[genre['description'] for genre in game_details['genres']]}")
    print(f"Price: {game_details['price_overview']['final_formatted']}" if 'price_overview' in game_details else "Free or not available for sale")
    print(f"Platforms: {', '.join([platform for platform, supported in game_details['platforms'].items() if supported])}")
else:
    print(game_details)
#Start of program
#if __init__ == "__main__":
    
#    main  
