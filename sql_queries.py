import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
log_data_s3 = config.get("S3","LOG_DATA")
song_data_s3 = config.get("S3","SONG_DATA")
# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create = ("""
create table if not exists staging_events(
                                        artist          varchar(500),
                                        auth            varchar(50),
                                        firstName       varchar(500),
                                        gender          char(1),
                                        iteminSession   integer,
                                        lastName        varchar(500),
                                        length          decimal(10,5),
                                        level           varchar(10),
                                        location        varchar(500),
                                        method          varchar(10),
                                        page            varchar(50),
                                        registration    BIGINT,
                                        sessionId       integer,
                                        song            varchar(500),
                                        status          integer,
                                        ts              BIGINT,
                                        userAgent       varchar(500),
                                        userId          integer
                                        );
""")
# {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null,
#  "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud",
#  "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = ("""
create table if not exists staging_songs(
                                        num_songs           integer,
                                        artist_id           varchar(50),
                                        artist_latitude     Decimal(9,6),
                                        artist_longitude    Decimal(9,6),
                                        artist_location     varchar(500),
                                        artist_name         varchar(500),
                                        song_id             varchar(50),
                                        title               varchar(500),
                                        duration            decimal(10,5),
                                        year                integer                                        
                                        );
""")

songplay_table_create = ("""
create table if not exists songplays(
                                    songplay_id integer identity(0, 1) PRIMARY KEY,
                                    start_time  timestamp not NULL,
                                    user_id     integer not NULL,
                                    level       varchar(10),
                                    song_id     varchar(50),
                                    artist_id   varchar(50),
                                    session_id  integer not NULL,
                                    location    varchar(500),
                                    user_agent  varchar(500)
                                    );
""")

user_table_create = ("""
create table if not exists users(
                                user_id     integer PRIMARY KEY,
                                first_name  varchar(500),
                                last_name   varchar(500),
                                gender      char(1),
                                level       varchar(10)
                                );
""")

song_table_create = ("""
create table if not exists songs(
                                song_id     varchar(50) PRIMARY KEY,
                                title       varchar(500),
                                artist_id   varchar(50),
                                year        integer,
                                duration    decimal(10,5)
                                );
""")

artist_table_create = ("""
create table if not exists artists(
                                artist_id   varchar(50) PRIMARY KEY,
                                name        varchar(500),
                                location    varchar(500),
                                lattitude   Decimal(9,6),
                                longitude   Decimal(9,6));
""")

time_table_create = ("""
create table if not exists time (
                                start_time  timestamp PRIMARY KEY,
                                hour        integer,
                                day         integer,
                                week        integer,
                                month       integer,
                                year        integer,
                                weekday     integer
                                );
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
format as json 'auto';
""").format(log_data_s3, ARN)

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
format as json 'auto';
""").format(song_data_s3, ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
create_dwh_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_dwh_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
