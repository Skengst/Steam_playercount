import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

conn = duckdb.connect('steam_database.duckdb')

def weekday_to_name(weekday_num):
  days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
  return days[weekday_num]



def page1():
    st.title("Steam Player Count Dashboard")

    query = """
    SELECT
        g.name,
        p.hour,
        p.date,
        strftime('%w', p.date) AS weekday,
        p.playercount
    FROM
        game_information g
    JOIN 
        playercount p
    ON 
        g.game_id = p.game_id
    """
    df = conn.execute(query).fetchdf()

    selected_names = st.multiselect("Select games to filter", options=df['name'].unique(), default=df['name'].unique())
    filtered_df = df[df['name'].isin(selected_names)]

    average_player_per_hour = filtered_df.groupby('hour')['playercount'].mean().reset_index()
 
    #Linechart
    st.line_chart(data=average_player_per_hour, x='hour', y='playercount', width=0, height=0)

    day_count = filtered_df.groupby('name')['date'].nunique().reset_index()

    player_count_per_game = filtered_df.groupby('name')['playercount'].sum().reset_index()

    player_count_per_game['avg_playercount_per_day'] = player_count_per_game['playercount'] / day_count['date']

    # Pie Chart
    fig = px.pie(
        player_count_per_game,
        values='avg_playercount_per_day',
        names='name',
        title='Average Player Count per Day',
        hover_data={'avg_playercount_per_day': True, 'name': True},
        labels={'avg_playercount_per_day': 'Avg Players/Day', 'name': 'Game Name'}
    )

    fig.update_traces(
        textinfo='percent+label',
        hovertemplate='%{label}: %{value:.0f} players/day (%{percent})'  # Format value with 0 decimal places
    )

    st.plotly_chart(fig)

def page2():
    st.subheader("Weekday based data")
    st.write("Shows total playercount based on weekday")
    query = """
    SELECT
        g.name,
        p.hour,
        p.date,
        strftime('%w', p.date) AS weekday,
        p.playercount
    FROM
        game_information g
    JOIN 
        playercount p
    ON 
        g.game_id = p.game_id
    """
    df = conn.execute(query).fetchdf()
    
    # Ensure 'weekday' is an integer type
    if df['weekday'].dtype != 'int64' and df['weekday'].dtype != 'int32':
        df['weekday'] = df['weekday'].astype(int)

    df['weekday_name'] = df['weekday'].apply(weekday_to_name)

    dates_per_weekday = df.groupby('weekday_name')['date'].nunique().reset_index()  
    pc_per_weekday = df.groupby('weekday_name')['playercount'].sum().reset_index()

    pc_per_weekday['pc_average_per_weekday'] = pc_per_weekday['playercount'] / dates_per_weekday['date']
    
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    all_weekdays = pd.DataFrame({'weekday_name': weekday_order})

    pc_per_weekday = all_weekdays.merge(pc_per_weekday, on='weekday_name', how='left')

    fig = px.bar(
        pc_per_weekday,
        x='weekday_name',
        y='pc_average_per_weekday',
        title='Player Count by Weekday',
        hover_data=['weekday_name', 'pc_average_per_weekday']
    )

    fig.update_traces(hovertemplate="<br>".join([
    "<b>Weekday:</b> %{x}",
    "<b>Average Player Count:</b> %{y:.2f}" 
    ]))

    fig.update_xaxes(title="Day of the Week") 
    fig.update_yaxes(title="Average Player Count") 
    st.plotly_chart(fig)


def page3():
    st.subheader("Based on genre")
    st.write("Content of page 3")
    query = """
    SELECT
        gg.genre,
        p.date,
        p.hour,
        p.playercount
    FROM
        game_genres gg
    JOIN
        game_information g ON gg.game_id = g.game_id
    JOIN
        playercount p ON g.game_id = p.game_id
    ORDER BY
        gg.genre,
        p.date,
        p.hour;
    """
    df = conn.execute(query).fetchdf()
    selected_genres = st.multiselect("Select genres to filter", options=df['genre'].unique(), default=df['genre'].unique())
    filtered_df = df[df['genre'].isin(selected_genres)]
    
    player_count_per_genre = filtered_df.groupby('genre')['playercount'].sum().reset_index()
    player_count_per_genre = player_count_per_genre.sort_values(by='playercount', ascending=False)

    fig_bar = px.bar(
        player_count_per_genre, 
        x='genre', 
        y='playercount', 
        title='Player Count by Genre') 
    st.plotly_chart(fig_bar)

# Page selection mechanism
page_names = ["Basic Data", "Weekday Data", "Genre Specific"]
selected_page = st.sidebar.radio("Select a Page", page_names)

if selected_page == "Basic Data":
    page1()
elif selected_page == "Weekday Data":
    page2()
elif selected_page == "Genre Specific":
    page3()
else:
    conn.close