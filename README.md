## Purpose:
To help Sparkify analyze data they are collecting on songs and user activities  
by building a star schema on RedShift DB that serves their needs.  

The goal is to understand what songs users are listening to!

## Database Schema & Design:
We built a star schema database to eliminate joins and simplify queries,  
the schema contains of five tables as below:  
- **Fact table**:
    1. **songplays**:
        1. songplay_id      (Auto increment integer)
        2. start_time
        3. user_id
        4. level
        5. song_id
        6. artist_id
        7. session_id
        8. location
        9. user_agent
- **Dimension tables**:
    1. **users**:
        1. user_id      (PK)
        2. first_name
        3. last_name
        4. gender
        5. level        
    2. **songs**:
        1. song_id      (PK)
        2. title
        3. artist_id
        4. year
        5. duration
    3. **artists**:
        1. artist_id    (PK)
        2. name
        3. location
        4. lattitude
        5. longitude
    4. **time**
        1. start_time   (PK)
        2. hour
        3. day
        4. week
        5. month
        6. year
        7. weekday
        
The **songplays** table will help us to accomplish our goal, because it obtains all PKs from all other dimension tables,  
so we can easy join with one of the dimension table to answer questions.  

- **ETL pipeline, how it works**:
    1. **Workflow**:
        1. Get all files for **song** and **log** data from the given paths
        2. process song files, by populating **song** and **artist** dimension tables
        3. process log files, by:
            1. populating **user** and **time** tables dimension tables
            2. populating **songplays** fact table
    
    2. **Populating Data:** The process is taking in consideration any duplicates occurs on table's PK,  
    by updating all columns instead of doing insert, example for table that might have duplicates while insert is **songs** table on **song_id** column.