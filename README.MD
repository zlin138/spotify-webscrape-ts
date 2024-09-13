## Project Overview

Inspired by Spotify Wrapped in December 2023, which showcased various analytics about user preferences and streaming patterns, this project aims to leverage publicly available data from Spotify. While user-specific data is kept private, Spotify releases daily information about top artists and top tracks. This project involves web scraping these charts for artist rankings and track streams across all available regions. The collected data serves as a foundation for a personal dashboard project, themed around Spotify's Top Artist of 2023, Taylor Swift, for simplicity and relevance.

### Environments

Environments for this project are managed using Anaconda. To create and install the required environments, use the following command:

```bash
conda create -f environment.yaml
```
#### .env File

Create a `.env` file in the root directory of your project with the following content:

```plaintext
# postgres credentials 
POSTGRES_USER=<your_postgres_user>
POSTGRES_PASSWORD=<your_postgres_password>
POSTGRES_DB=<your_postgres_db>
DB_HOST=localhost
DB_PORT=5432

# Spotify credentials
SPOTIFY_USERNAME=<your_spotify_username>
SPOTIFY_PASSWORD=<your_spotify_password>
```

Replace `<your_postgres_user>`, `<your_postgres_password>`, `<your_postgres_db>`, `<your_spotify_username>`, and `<your_spotify_password>` with your actual credentials. This file will be used to configure both your database and Spotify connection settings.

### Start the Docker Container

To start the Docker container in detached mode (background), use:

```bash
docker compose -f docker-compose.yaml up -d
```

- `-f docker-compose.yaml` specifies the Docker Compose file.
- `-d` runs the container in detached mode.

### Stop the Docker Container

To stop and remove the Docker container, use:

```bash
docker compose -f docker-compose.yaml down
```

- This command stops and removes the container.

**Important:** Keep the PostgreSQL Docker container running whenever you need to connect to the database or insert data. Make sure the container is up before performing any data operations.


### Running the Script and Results

To run the `spotify.py` script:

```bash
python spotify.py
```

This script parses the top 200 songs and top 200 artists across all available regions and stores the data back into the database for yesterday. You can use a scheduler to run this script daily, but it's also possible to modify the existing code to backfill past data. Make sure to schedule the script to run after Spotify has updated their page.

The script `kworb.py` stores the top 500 daily songs for a specific artist (This is artist-specific and includes a much greater breadth of data beyond Spotify's top 200 songs). However, the site does not have any backfill abilities as the site is refreshed daily with no logs.

I've included a sample SQL script `tables.sql` which contains sample tables and the associated data columns for reference. The current code in `spotify.py` and `kworb.py` is compatible with the data defined. You can also fill in more detailed information using Spotify's API for album and track information.


### Dashboarding

A comprehensive dashboard for Taylor Swift was created in Tableau using data from this repo, but not all the data from the dashboard came directly from Spotify. *Currently in the process of migrating the data(ie change source to database) behind these dashboards and publishing to Tableau Cloud.*

Check out the results [here](https://public.tableau.com/app/profile/zhang.lin2425/viz/SpotifySwift/SpotifyDebutDashboard).



