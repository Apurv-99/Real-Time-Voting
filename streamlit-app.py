import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import json
import streamlit as st
from confluent_kafka import Consumer
from streamlit_autorefresh import st_autorefresh
import psycopg2

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CANDIDATE_VOTES_TOPIC = 'aggregated_votes_per_candidate'
LOCATION_TURNOUT_TOPIC = 'turnout_by_location'
DB_CONNECTION_STRING = "host=localhost dbname=voting user=postgres password=postgres"


@st.cache_resource
def create_kafka_consumer(topic_name):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'real-time-dashboard-group-main',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([topic_name])
    return consumer

@st.cache_data(ttl=180)
def fetch_master_vote_data():
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    query = """
        SELECT
            c.candidate_id,
            c.candidate_name,
            c.party_affiliation,
            c.photo_url,
            COALESCE(COUNT(v.voter_id), 0) AS total_votes
        FROM
            candidates c
        LEFT JOIN
            votes v ON c.candidate_id = v.candidate_id
        GROUP BY
            c.candidate_id, c.candidate_name, c.party_affiliation, c.photo_url
        ORDER BY
            c.candidate_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def fetch_voting_stats():
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        cur.execute("SELECT count(*) voters_count FROM voters")
        voters_count = cur.fetchone()[0]

        cur.execute("SELECT count(*) candidates_count FROM candidates")
        candidates_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        return voters_count, candidates_count
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return 0, 0

def fetch_data_from_kafka(consumer):
    data = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break
        if msg.error():
            st.warning(f"Kafka Error: {msg.error()}")
            break
        try:
            data.append(json.loads(msg.value().decode('utf-8')))
        except json.JSONDecodeError as e:
            st.error(f"Error decoding Kafka message: {e}")
    return data


def plot_colored_bar_chart(results):
    fig, ax = plt.subplots(figsize=(10, 6))
    results = results.sort_values(by='total_votes', ascending=False)
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    ax.bar(data_type, results['total_votes'], color=colors)
    ax.set_xlabel('Candidate')
    ax.set_ylabel('Total Votes')
    ax.set_title('Vote Counts per Candidate')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return fig

def plot_donut_chart(data: pd.DataFrame, title='Donut Chart'):
    if data.empty or 'total_votes' not in data.columns or data['total_votes'].sum() == 0:
        return None
    fig, ax = plt.subplots(figsize=(8, 8))
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])
    colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))
    wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                      startangle=140, colors=colors, wedgeprops=dict(width=0.3))
    ax.axis('equal')
    plt.title(title)
    plt.tight_layout()
    return fig


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    if 'results_df' not in st.session_state:
        st.session_state.results_df = fetch_master_vote_data()

    candidate_consumer = create_kafka_consumer(CANDIDATE_VOTES_TOPIC)
    candidate_data = fetch_data_from_kafka(candidate_consumer)

    if candidate_data:
        updates_df = pd.DataFrame(candidate_data)
        updates_df = updates_df.groupby('candidate_id').last().reset_index()

        current_results = st.session_state.results_df.set_index('candidate_id')
        updates_df = updates_df.set_index('candidate_id')
        
        current_results.update(updates_df)
        
        st.session_state.results_df = current_results.reset_index()
    
    
    results = st.session_state.results_df

    
    voters_count, candidates_count = fetch_voting_stats()
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    
    st.markdown("""---""")
    st.header('Leading Candidate')
    if not results.empty:
        leading_candidate = results.loc[results['total_votes'].idxmax()]
        col1, col2 = st.columns(2)
        with col1:
            st.image(leading_candidate['photo_url'], width=200)
        with col2:
            st.header(leading_candidate['candidate_name'])
            st.subheader(leading_candidate['party_affiliation'])
            st.subheader(f"Total Votes: {int(leading_candidate['total_votes']):,}")

    st.markdown("""---""")
    st.header('Statistics')
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(results, title='Vote Distribution')
        if donut_fig:
            st.pyplot(donut_fig)

    st.dataframe(results[['candidate_name', 'party_affiliation', 'total_votes']])


def sidebar():
    st.sidebar.title("Controls")
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button('Force Refresh from DB'):
        if 'results_df' in st.session_state:
            del st.session_state.results_df
        st.cache_data.clear()
        st.rerun()

st.title('Real-time Election Dashboard')
sidebar()
update_data()