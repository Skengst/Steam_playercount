#Imports
import requests
import pandas as pd
import duckdb
from datetime import datetime

#Variables
API_key = "8D2C0A79C294D216990333F6D68CE577"
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
    
    # Create the player count table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playercount (
            id INTEGER,              -- Unique identifier for each entry
            game_id INTEGER,         -- Steam App ID of the game
            hour INTEGER,            -- Hour of the player count record
            date DATE,               -- Date of the player count record
            playercount INTEGER      -- Player count at the given time
        )
    """)
    
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
