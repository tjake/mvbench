//Manual Schema
CREATE KEYSPACE manual WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE manual.user_playlists
(
    user_name           text,
    playlist_name       text,
    song_id             text,
    added_time          bigint,
    artist_name         text,
    genre               text,
    last_played         bigint,
    PRIMARY KEY (user_name, playlist_name, song_id) 
);

CREATE TABLE manual.song_to_user
(
    song_id             text,
    user_name           text,
    playlist_name       text,
    added_time          bigint,
    PRIMARY KEY (song_id, user_name, playlist_name)
);


CREATE TABLE manual.artist_to_user
(
    artist_name         text,
    user_name           text,
    playlist_name       text,
    song_id             text,
    PRIMARY KEY(artist_name, user_name, playlist_name, song_id)
);

CREATE TABLE manual.genre_to_user
(
    genre               text,
    user_name           text,
    playlist_name       text,
    song_id             text,
    PRIMARY KEY(genre, user_name, playlist_name, song_id)
);

CREATE TABLE manual.recently_played
(
    last_played         bigint,
    user_name           text,
    playlist_name       text,
    song_id             text,
    PRIMARY KEY (last_played, user_name, playlist_name, song_id)
);




//////////////////////////////////////////////////////////////////
//Materialized View Schema
CREATE KEYSPACE mview WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE mview.user_playlists
(
    user_name  text,
    playlist_name  text,
    song_id  text,
    added_time 	bigint,
    artist_name text,
    genre  text,
    last_played bigint,
    PRIMARY KEY (user_name, playlist_name, song_id)
);

CREATE MATERIALIZED VIEW mview.song_to_user AS
      SELECT song_id, user_name, added_time
      FROM user_playlists
      WHERE song_id IS NOT NULL
      AND playlist_name IS NOT NULL
      AND user_name IS NOT NULL
      PRIMARY KEY(song_id, user_name, playlist_name);

CREATE MATERIALIZED VIEW mview.artist_to_user AS
      SELECT song_id, user_name
      FROM user_playlists
      WHERE song_id IS NOT NULL
      AND playlist_name IS NOT NULL
      AND user_name IS NOT NULL
      AND artist_name IS NOT NULL
      PRIMARY KEY(artist_name, user_name, playlist_name, song_id);


CREATE MATERIALIZED VIEW mview.genre_to_user AS
      SELECT song_id, user_name
      FROM user_playlists
      WHERE song_id IS NOT NULL
      AND playlist_name IS NOT NULL
      AND user_name IS NOT NULL
      AND genre IS NOT NULL
      PRIMARY KEY(genre, user_name, playlist_name, song_id);


CREATE MATERIALIZED VIEW mview.recently_played AS
      SELECT song_id, user_name
      FROM user_playlists
      WHERE song_id IS NOT NULL
      AND playlist_name IS NOT NULL
      AND user_name IS NOT NULL
      AND last_played IS NOT NULL
      PRIMARY KEY(user_name, last_played, playlist_name, song_id);


