import random
import time
from datetime import datetime
import psycopg2
import json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def get_candidate_data(cursor):
    cursor.execute("""
        SELECT row_to_json(t)
        FROM (
            SELECT candidate_id, candidate_name, party_affiliation FROM candidates
        ) t;
    """)
    candidates = [candidate[0] for candidate in cursor.fetchall()]
    if not candidates:
        raise Exception("No candidates found in the database. Please populate the 'candidates' table.")
    return candidates

if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cursor = conn.cursor()
        candidates = get_candidate_data(cursor)
        producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'voting-processor-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        consumer.subscribe(['voters'])
        print("Vote processing script started. Waiting for new voters...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            

            voter = json.loads(msg.value().decode('utf-8'))
            chosen_candidate = random.choice(candidates)
            
            vote = {
                "voter_id": voter.get('voter_id'),
                "candidate_id": chosen_candidate.get('candidate_id'),
                "voting_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "vote": 1
            }

            try:
                print(f"Processing vote for Voter ID: {vote['voter_id']} -> Candidate ID: {vote['candidate_id']}")
                
                cursor.execute("""
                    INSERT INTO votes (voter_id, candidate_id, voting_time)
                    VALUES (%s, %s, %s)
                """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                conn.commit()

                producer.produce(
                    topic='votes',
                    key=str(vote["voter_id"]),
                    value=json.dumps(vote),
                    on_delivery=delivery_report
                )
                producer.poll(0)
                consumer.commit(message=msg)

            except Exception as e:
                print(f"Error processing vote for Voter ID {vote.get('voter_id')}: {e}")
                conn.rollback()

    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'consumer' in locals() and consumer:
            consumer.close()
        if conn:
            cursor.close()
            conn.close()
        print("Connections closed.")