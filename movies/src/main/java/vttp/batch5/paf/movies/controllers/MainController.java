package vttp.batch5.paf.movies.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import jakarta.json.JsonArray;
import vttp.batch5.paf.movies.services.MovieService;

@Controller
@RequestMapping("/api/summary")
public class MainController {

    @Autowired
    MovieService movieSvc;

    Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

    // TODO: Task 3
    @GetMapping()
    public ResponseEntity<String> getProlificDirectors(@RequestParam int count) {
        JsonArray response = movieSvc.getProlificDirectors(count);
        return ResponseEntity.ok().body(response.toString());
    }
   
    // TODO: Task 4
    @GetMapping("/pdf")
    public String getMethodName(@RequestParam String count) {
        try {
            movieSvc.generatePDFReport();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return "forward:/index";
    }
}
