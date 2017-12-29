CREATE VIEW IF NOT EXISTS moviesAgg AS
SELECT movieId, count(movieId) as counts 
FROM movies
GROUP BY movieid;

SELECT n.name, counts 
FROM moviesagg m JOIN name n ON n.movieid = m.movieid
order by counts DESC; 
