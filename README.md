# YouTube Data Harvesting and Warehousing

## Description
This is a Streamlit-based application for collecting, storing, and analyzing YouTube data using YouTube API. The collected data is stored in MongoDB and later migrated to PostgreSQL for further analysis.

## Features
- Extracts channel, playlist, video, and comment details from YouTube.
- Stores data in **MongoDB** and migrates it to **PostgreSQL**.
- Provides a Streamlit dashboard to explore and analyze YouTube data.
- Displays top videos, comments, and statistics for various channels.
- Enables SQL-based querying for deep insights.

## Technologies Used
- **Python**
- **Streamlit**
- **YouTube API (Google API)**
- **MongoDB (pymongo)**
- **PostgreSQL (psycopg2)**
- **Pandas**

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repo.git
Install dependencies:

bash
Copy
Edit
pip install -r requirements.txt
Run the application:

bash
Copy
Edit
streamlit run project.py
Database Setup
MongoDB
Ensure you have a MongoDB cluster and update the connection string in project.py:

python
Copy
Edit
client = pymongo.MongoClient('your-mongodb-connection-string')
The database name is youtube_data.

PostgreSQL
Create a PostgreSQL database named youtube_data.

Update the mydb connection details in project.py with your credentials.

Tables will be automatically created when migrating data from MongoDB.

Usage
Enter a YouTube Channel ID to fetch and store data.

Click "Collect and Store Data" to save the channel data in MongoDB.

Select a channel and click "Migrate to SQL" to transfer data to PostgreSQL.

Explore data using the SQL Query Panel in the Streamlit app.

License
This project is licensed under the MIT License.

Contact
For any queries, contact ANAND L
