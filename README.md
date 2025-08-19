<img width="1000" height="1000" alt="image" src="https://github.com/user-attachments/assets/28a83bcd-3e8b-483b-bf2b-cdeff65a65e5" />


## System Components
- **main.py**: This is the main Python script that creates the required tables on postgres (`candidates`, `voters` and `votes`), it also creates the Kafka topic and creates a copy of the `votes` table in the Kafka topic.
- **voting.py**: This is the Python script that contains the logic to consume the voters from the Kafka topic (`voters`), generate voting data and produce data to `votes` topic on Kafka.
- **spark-streaming.py**: This is the Python script that contains the logic to consume the votes from the Kafka topic (`votes`), enrich the data from postgres and aggregate the votes and produce data to specific topics on Kafka.
- **streamlit-app.py**: This is the Python script that contains the logic to consume the aggregated voting data from the Kafka topic as well as postgres and display the voting data in realtime using Streamlit.

## Setting up the System
This Docker Compose file allows you to easily spin up Zookkeeper, Kafka, Spark master, Spark worker and Postgres application in Docker containers.

### Prerequisites
- Python 3.9 or above installed on your machine
- Docker installed on your machine
- Pyspark version 4.0.0 is used
  
## Screenshots

<img width="1000" height="1000" alt="image" src="https://github.com/user-attachments/assets/26097729-64b6-48b0-8d9a-595e6faa97fc" />

<img width="500" height="500" alt="image" src="https://github.com/user-attachments/assets/fc66ff8d-a033-4c04-9ad8-3683ef5bb972" />

