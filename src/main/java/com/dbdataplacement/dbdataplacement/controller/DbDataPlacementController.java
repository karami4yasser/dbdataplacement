package com.dbdataplacement.dbdataplacement.controller;
import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import com.dbdataplacement.dbdataplacement.utils.TableProperties;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Validated
@RestController
@RequestMapping(  "/api")
@CrossOrigin(origins = "*")
public class DbDataPlacementController {

    private final DBUtils dbUtils;



    public DbDataPlacementController(DBUtils dbUtils) {

        this.dbUtils = dbUtils;
    }

    @PostMapping("/archive")
    public boolean moveDataToArchiveTableSelectInsert(
    ) {

      return dbUtils.moveDataAllTables();
    }

}
