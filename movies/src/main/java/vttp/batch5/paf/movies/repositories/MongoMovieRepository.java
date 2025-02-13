package vttp.batch5.paf.movies.repositories;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.constants.CollectionNames;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

    Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    //
    //      db.imdb.insertMany([
    //          {...},
    //          {...}
    //      ])
    //
    // Inserts in a batch documents
    public void batchInsertMovies(List<Document> movieBatch, String collectionName) {
        Collection<Document> insertedDocs = mongoTemplate.insert(movieBatch, collectionName);
    }

    public List<Document> parseMovieData(List<JsonObject> movieBatch) {
        List<Document> parsedMovieBatch = new ArrayList<>();
        for (int i = 0; i < movieBatch.size(); i++) {
            Document rawMovieDoc = Document.parse(movieBatch.get(i).toString());
            Map<String, Object> movieMap = new HashMap<>();
            movieMap.put("_id", rawMovieDoc.get("imdb_id"));
            movieMap.put("title", rawMovieDoc.get("title"));
            movieMap.put("director", rawMovieDoc.get("director"));
            movieMap.put("overview", rawMovieDoc.get("overview"));
            movieMap.put("tagline", rawMovieDoc.get("tagline"));
            movieMap.put("genres", rawMovieDoc.get("genres"));
            movieMap.put("imdb_rating", rawMovieDoc.get("imdb_rating"));
            movieMap.put("imdb_votes", rawMovieDoc.get("imdb_votes"));
            Document movieDoc = new Document(movieMap);
            parsedMovieBatch.add(movieDoc);
        }
        return parsedMovieBatch;
    }
    
    // TODO: Task 2.4
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    //
    //    db.errors.insert({
    //        ids: [ 123, abc, etc ]
    //        error: "...",
    //        timestamp: $$NOW
    //    })
    //
    public void logError(List<JsonObject> movieBatch, Exception e) {
        Map<String, Object> errorMap = new HashMap<>();
        Date timestamp = new Date();
        JsonArray ids = getMovieBatchIds(movieBatch);

        errorMap.put("ids", ids);
        errorMap.put("error", e.getMessage());
        errorMap.put("timestamp", timestamp);

        Document errorDoc = new Document(errorMap);

        mongoTemplate.insert(errorDoc, CollectionNames.MONGO_ERRORS);
    }
    
    private JsonArray getMovieBatchIds(List<JsonObject> movieBatch) {
        JsonArrayBuilder jsonArrBldr = Json.createArrayBuilder();
        for (int i = 0; i < movieBatch.size(); i++) {
            JsonObject movie = movieBatch.get(i);
            jsonArrBldr.add(movie.getString("imdb_id"));
        }
        return jsonArrBldr.build();
    }

    // TODO: Task 3
    // Write the native Mongo query you implement in the method in the comments
    /*     
        db.imdb.aggregate([
            { $match: {
                "director": { "$nin" : [null, ""] }
            } },
            {
                $group: {
                    "_id": "$director",
                    "count": { $sum: 1 },
                    "imdb_id": { $push: "$_id" }
                }
            },
            { $sort: { count: -1 } },
            { $limit: 8 }
        ])
    */
    public List<Document> getTopDirectorMovies(int count) {
        MatchOperation matchNonEmptyStrings = Aggregation.match(
            Criteria.where("director").nin(null, "")
        );

        GroupOperation groupOperation = Aggregation.group("director")
            .count().as("count")
            .push("_id").as("imdb_id");

        SortOperation sortByMovieCount = Aggregation.sort(
            Sort.by(Direction.DESC, "count")
        );

        LimitOperation limitDirectors = Aggregation.limit(count);

        Aggregation pipeline = Aggregation.newAggregation(
            matchNonEmptyStrings,
            groupOperation,
            sortByMovieCount,
            limitDirectors
        );

        AggregationResults<Document> results = mongoTemplate.aggregate(
            pipeline, CollectionNames.MONGO_IMDB, Document.class);

        return results.getMappedResults();
    }
}
