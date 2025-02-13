DROP DATABASE IF EXISTS movies;
CREATE DATABASE movies;

USE movies;

DROP TABLE IF EXISTS imdb;
CREATE TABLE imdb (
    imdb_id VARCHAR(16) NOT NULL,
    vote_average FLOAT DEFAULT 0,
    vote_count INT DEFAULT 0,
    release_date DATE NOT NULL,
    revenue DECIMAL(15,2) DEFAULT 1000000,
    budget DECIMAL(15,2) DEFAULT 1000000,
    runtime INT DEFAULT 90,

    CONSTRAINT pk_imdb_id PRIMARY KEY(imdb_id)
);