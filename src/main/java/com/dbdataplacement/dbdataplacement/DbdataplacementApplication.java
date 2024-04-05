package com.dbdataplacement.dbdataplacement;


import com.dbdataplacement.dbdataplacement.utils.TableProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.Map;


@SpringBootApplication
public class DbdataplacementApplication{

    public static void main(String[] args) {
        SpringApplication.run(DbdataplacementApplication.class, args);
    }

}
