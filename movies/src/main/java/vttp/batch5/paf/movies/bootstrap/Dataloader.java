package vttp.batch5.paf.movies.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.services.MovieService;

@Component
public class Dataloader implements CommandLineRunner {

  @Autowired
  MovieService movieSvc;

  @Autowired
  MySQLMovieRepository mySQLMovieRepo;

  // TODO: remove
  @Autowired
  MongoMovieRepository mongoMovieRepository;

  Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

  // TODO: Task 2
  @Override
  public void run(String... args) {
    try {
      List<JsonObject> movieData = loadJson();
      movieSvc.insertMovies(movieData);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    logger.info("All data files loaded.");

    
    movieSvc.getProlificDirectors(8);
  }

  private List<JsonObject> loadJson() throws IOException {
    logger.info("Loading ZIP file");
    Path p = Paths.get("src/main/resources/static/movies_post_2010.zip");
    ZipFile zipFile = new ZipFile(p.toFile());
    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    while(entries.hasMoreElements()){
      ZipEntry entry = entries.nextElement();

      try (InputStream is = zipFile.getInputStream(entry)) {
        InputStreamReader isr = new InputStreamReader(is);
        LineNumberReader lnr = new LineNumberReader(isr);

        List<JsonObject> filteredMovies = new ArrayList<>();
        
        String line;
        logger.info("Reading JSON file");
        while (null != (line = lnr.readLine())) {
          JsonReader jr = Json.createReader(new StringReader(line));
          JsonObject movieObject = jr.readObject();

          try {
            String releaseDate = movieObject.getString("release_date");
            if (filterReleaseDate(releaseDate, 2018)) {
              filteredMovies.add(movieObject);
            }
          } catch (Exception e) {
            logger.warn("Erroneous release date read, replacing with false");
            movieObject.put("release_date", JsonValue.FALSE);
            filteredMovies.add(movieObject);
          }
        }
        logger.info("Reading of JSON file complete.");
        return filteredMovies;
      } catch (Exception e) {
        logger.error(e.getMessage());
        throw new IOException();
      }
    }
    throw new IOException();
  }

  // Returns true if release date is including or after the filter year
  private boolean filterReleaseDate(String releaseDate, int filterYear) {
    String[] dateArr = releaseDate.split("-");
    return Integer.parseInt(dateArr[0]) >= filterYear;
  }

}
