package com.dbdataplacement.dbdataplacement.controller;

import com.dbdataplacement.dbdataplacement.dto.MoveDataToArchiveTableRequestDto;
import com.dbdataplacement.dbdataplacement.service.DbDataPlacementServiceInsertSelect;
import com.dbdataplacement.dbdataplacement.service.interfaces.DbDataPlacementService;
import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

@Validated
@RestController
@RequestMapping(  "/api")
@CrossOrigin(origins = "*")
public class DbDataPlacementController {

    private final DbDataPlacementService dbDataPlacementServiceInsertSelect;


    public DbDataPlacementController(DBUtils dbUtils) {
        this.dbDataPlacementServiceInsertSelect = new DbDataPlacementServiceInsertSelect(dbUtils);
    }

    @PostMapping("/archive")
    public boolean moveDataToArchiveTableSelectInsert(
             @RequestBody MoveDataToArchiveTableRequestDto dto
    ) throws SQLException, IOException, ExecutionException, InterruptedException {

      return dbDataPlacementServiceInsertSelect.moveDataToArchiveTable(dto.sourceTable(), dto.destinationTable());
    }

}
