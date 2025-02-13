package vttp.batch5.paf.movies.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import jakarta.json.JsonArray;
import vttp.batch5.paf.movies.services.MovieService;



@Controller
@RequestMapping("/api")
public class MainController {

    @Autowired
    MovieService movieSvc;

    // TODO: Task 3
    @GetMapping("/summary")
    public ResponseEntity<String> getProlificDirectors(@RequestParam int count) {
        JsonArray response = movieSvc.getProlificDirectors(count);
        return ResponseEntity.ok().body(response.toString());
    }
   

  
    // TODO: Task 4


}
