get_average_rating_from_each_genre = """SELECT name, AVG(AverageRating) FROM movies.moviegenre
inner join movies.genre on movies.moviegenre.GenreId=movies.genre.GenreId
inner join movies.movie on movies.moviegenre.MovieId=movies.movie.MovieId
group by name order by AVG(AverageRating) desc"""

get_average_rating_from_each_year = """SELECT year, AVG(AverageRating) FROM movies.movie group by year order by AVG(AverageRating) desc"""