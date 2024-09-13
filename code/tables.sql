CREATE TABLE artist_rank (
    date DATE,
    region VARCHAR(255),
    position INTEGER,
    change VARCHAR(50),
    artist_name VARCHAR(255),
    peak INTEGER,
    prev VARCHAR(10),
    streak INTEGER,
    PRIMARY KEY (artist_name, date, region, position)
);

CREATE TABLE song_streams (
    date DATE,
    region VARCHAR(255),
    position INTEGER,
    change VARCHAR(50),
    track_title VARCHAR(255), 
    artist_name VARCHAR(255), 
    peak INTEGER,
    prev VARCHAR(10),
    streak INTEGER,
    streams BIGINT,
    PRIMARY KEY (track_title, date, region, position)
);

CREATE TABLE daily_streams( 
    date DATE, 
    track_title VARCHAR(255), 
    total_streams BIGINT, 
    daily_streams INT, 
    PRIMARY KEY (date, track_title)
)

/* This part is not used in the code just for me to import data*/ 
CREATE TABLE artists (
    artist_id SERIAL PRIMARY KEY,
    artist_name VARCHAR(100) NOT NULL
);

-- Create the albums table with a foreign key to artists
CREATE TABLE albums (
    album_id SERIAL PRIMARY KEY,
    album_name VARCHAR(100) NOT NULL,
    artist_id INT NOT NULL,
    release_date DATE,
    track_length INT,
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

CREATE TABLE tracklist (
    track_id SERIAL PRIMARY KEY,
    track_title TEXT NOT NULL,
    album_id INT NOT NULL,
    track_number INT,
    FOREIGN KEY (album_id) REFERENCES albums(album_id)
);