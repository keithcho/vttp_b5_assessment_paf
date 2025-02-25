package vttp.batch5.paf.movies.services;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.export.SimpleExporterInput;
import net.sf.jasperreports.export.SimpleOutputStreamExporterOutput;
import net.sf.jasperreports.json.data.JsonDataSource;
import net.sf.jasperreports.pdf.JRPdfExporter;
import net.sf.jasperreports.pdf.SimplePdfExporterConfiguration;
import net.sf.jasperreports.pdf.SimplePdfReportConfiguration;
import vttp.batch5.paf.movies.constants.CollectionNames;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  MongoMovieRepository mongoMovieRepo;

  @Autowired
  MySQLMovieRepository mySQLMovieRepo;

  Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

  // TODO: Task 2
  public void insertMovies(List<JsonObject> movieData) {
    // Create a list to cache batches of 25 movies
    List<JsonObject> movieBatch = new ArrayList<>();
    int insertCount = 0;

    for (int i = 0; i < movieData.size(); i++) {
      // Add each movie to the cache
      movieBatch.add(movieData.get(i));
      insertCount++;

      // Once the cache has 25 movies, insert into MySQL and MongoDB
      if (movieBatch.size() == 25) {
        try {
          try {
            // Insert batch into MySQL
            int[] added = mySQLMovieRepo.batchInsertMovies(movieBatch);
            logger.info("Inserted batch of " + movieBatch.size() + " into MySQL");
            logger.info(Arrays.toString(added));
          } catch (Exception e) {
            logger.error("Error inserting into MySQL: " + e.getMessage());
            mongoMovieRepo.logError(movieBatch, e);
          }
          
          try {
            // Insert batch into MongoDB
            List<Document> mongoMovieBatch = mongoMovieRepo.parseMovieData(movieBatch);
            mongoMovieRepo.batchInsertMovies(mongoMovieBatch, CollectionNames.MONGO_IMDB);
            logger.info("Inserted batch of " + movieBatch.size() + " into MongoDB");
          } catch (Exception e) {
            logger.error("Error inserting into MongoDB: " + e.getMessage());
            mongoMovieRepo.logError(movieBatch, e);
          }
          
          // Empty list after inserting into db
          movieBatch = new ArrayList<>();
        } catch (Exception e) {
          // Log errors and empty cache of movies
          logger.error(e.getMessage());
          insertCount = 0;
          movieBatch = new ArrayList<>();
        }
      }
    }
    // Insert remaining unbatched movies (<25)
    try {
      // Insert batch into MySQL
      mySQLMovieRepo.batchInsertMovies(movieBatch);
      logger.info("Inserted batch of " + movieBatch.size() + " into MySQL");
    } catch (Exception e) {
      logger.error("Error inserting into MySQL: " + e.getMessage());
      mongoMovieRepo.logError(movieBatch, e);
    }
    try {
      // Insert batch into MongoDB
      List<Document> mongoMovieBatch = mongoMovieRepo.parseMovieData(movieBatch);
      mongoMovieRepo.batchInsertMovies(mongoMovieBatch, CollectionNames.MONGO_IMDB);
      logger.info("Inserted batch of " + movieBatch.size() + " into MongoDB");
    } catch (Exception e) {
      logger.error("Error inserting into MongoDB: " + e.getMessage());
      mongoMovieRepo.logError(movieBatch, e);
    }
  }

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public JsonArray getProlificDirectors(int count) {

    // Get top directors from MongoDB, and the list of movies they directed
    List<Document> topDirectors = mongoMovieRepo.getTopDirectorMovies(count);

    JsonArrayBuilder response = Json.createArrayBuilder();

    // Iterate through each director
    for (int i = 0; i < topDirectors.size(); i++) {
        Document directorDoc = topDirectors.get(i);
        List<String> movieIdArray = directorDoc.get("imdb_id", ArrayList.class);

        float directorRevenue = 0;
        float directorBudget = 0;
        // Iterate through each movie
        for (int j = 0; j < movieIdArray.size(); j++) {
          List<Object[]> sqlResults = mySQLMovieRepo.getRevenueBudgetByMovie(movieIdArray.get(j));

          // Get revenue and budget for each movie
          for (Object[] elem : sqlResults) {
              directorRevenue += Float.parseFloat(elem[0].toString());
              directorBudget += Float.parseFloat(elem[1].toString());
          }
        }

        // Add data for the director to JSON response
        JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder();
        jsonObjectBuilder.add("director_name", directorDoc.getString("_id"));
        jsonObjectBuilder.add("movies_count", directorDoc.getInteger("count"));
        jsonObjectBuilder.add("total_revenue", directorRevenue);
        jsonObjectBuilder.add("total_budget", directorBudget);
        response.add(jsonObjectBuilder.build());
    }
    
    return response.build();
  }

  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() throws FileNotFoundException, JRException {
    Path p = Paths.get("src/main/resources/static/director_movies_report.jrxml");

    JsonDataSource reportDS = new JsonDataSource(new File("report.pdf"));

    JsonDataSource directorsDS = new JsonDataSource(p.toFile());

    Map<String, Object> params = new HashMap<>();
    params.put("DIRECTOR_TABLE_DATASET", directorsDS);

    InputStream reportStream = getClass().getResourceAsStream(p.toString());
    JasperReport report = JasperCompileManager.compileReport(reportStream);

    JasperPrint print = JasperFillManager.fillReport(report, params, reportDS);

    JRPdfExporter exporter = new JRPdfExporter();
    exporter.setExporterInput(new SimpleExporterInput(print));
    exporter.setExporterOutput(new SimpleOutputStreamExporterOutput("employeeReport.pdf"));
    SimplePdfReportConfiguration reportConfig = new SimplePdfReportConfiguration();

    reportConfig.setSizePageToContent(true);
    reportConfig.setForceLineBreakPolicy(false);

    SimplePdfExporterConfiguration exportConfig = new SimplePdfExporterConfiguration();
    exportConfig.setEncrypted(true);
    exportConfig.setAllowedPermissionsHint("PRINTING");
    exporter.setConfiguration(reportConfig);
    exporter.setConfiguration(exportConfig);
    exporter.exportReport();
  }
}
