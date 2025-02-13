package vttp.batch5.paf.movies.repositories;

import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.JsonObject;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate template;

    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    @Transactional
    public int[] batchInsertMovies(List<JsonObject> movieBatch) throws ParseException {
        String SQLQuery = 
        """
            INSERT INTO imdb
                (imdb_id, vote_average, vote_count, release_date,
                revenue, budget, runtime)
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
        """;

        List<Object[]> params = new ArrayList<>();
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        for (int i = 0; i < movieBatch.size(); i++) {
            JsonObject movie = movieBatch.get(i);
            
            Date releaseDate = new Date(formatter.parse(movie.getString("release_date")).getTime());

            Object[] movieParams = new Object[] {
                movie.getString("imdb_id"),
                movie.getInt("vote_average"),
                movie.getInt("vote_count"),
                releaseDate,
                movie.getInt("revenue"),
                movie.getInt("budget"),
                movie.getInt("runtime"),
            };
            params.add(movieParams);
        }
        int added[] = template.batchUpdate(SQLQuery, params);
            
        return added;
    }

    // TODO: Task 3
    public List<Object[]> getRevenueBudgetByMovie(String imdbId) {
        String SQLQuery = 
        """
            SELECT revenue, budget
                FROM imdb
                WHERE imdb_id LIKE ?
        """;

        List<Object[]> result = template.queryForList(SQLQuery, imdbId).stream()
            .map(row -> row.values().toArray()).collect(Collectors.toList());

        return result;
    }

}
