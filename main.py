import psycopg2
import requests
from confluent_kafka import SerializingProducer
import json

def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            photo_url TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            country VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()

def generate_candidates(cursor, conn, number):
    cursor.execute("DELETE FROM candidates")
    PARTIES = ["Party A", "Party B", "Party C", "Party D", "Party E"]
    for i in range(number):
        response = requests.get(f"https://randomuser.me/api/?nat=in")
        if response.status_code == 200:
            user_data = response.json()['results'][0]
            candidate_id =  user_data['login']['uuid']
            candidate_name = f"{user_data['name']['first']} {user_data['name']['last']}"
            party_affiliation = PARTIES[i]
            biography = "A brief bio of the candidate."
            photo_url = user_data['picture']['large']

            cursor.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, photo_url)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                candidate_id, candidate_name, party_affiliation, biography, photo_url))
            conn.commit()
        else:
            raise Exception("Failed to fetch candidate data from randomuser.me")


def generate_voters(cursor, conn):
    response = requests.get(f"https://randomuser.me/api/?nat=in")
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        voter_data = {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "city": user_data['location']['city'],
            "state": user_data['location']['state'],
            "country": user_data['location']['country'],
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
        cursor.execute("""
            INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, city, state, country, email, phone_number, picture, registered_age)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            voter_data["voter_id"],
            voter_data["voter_name"],
            voter_data["date_of_birth"],
            voter_data["gender"],
            voter_data["city"],
            voter_data["state"],
            voter_data["country"],
            voter_data["email"],
            voter_data["phone_number"],
            voter_data["picture"],
            voter_data["registered_age"]
        ))
        conn.commit()
        return voter_data
    else:
        raise Exception("Failed to fetch voter data from randomuser.me")

def delivery_report(err, msg):
    if err is None:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    else:
        print(f"Failed to deliver message: {err}")

if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="voting",
        user="postgres",
        password="postgres"
    )
    cursor = conn.cursor()

    create_tables(conn, cursor) 

    cursor.execute("SELECT * FROM candidates")
    candidates = cursor.fetchall()
    print(candidates)

    if len(candidates) == 0:
        generate_candidates(cursor, conn, 5)

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })


    for i in range(1000):
        voter_data = generate_voters(cursor, conn)
        producer.produce(
            topic="voters",
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )