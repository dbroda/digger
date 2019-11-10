package com.codeplus.digger.examples.app;

import com.codeplus.digger.core.Digger;
import io.vavr.collection.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class DataGeneratorConsoleApplication implements CommandLineRunner {

    public static final String STARTING_NODE = "event";

    //    @Autowired
//    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... args) throws Exception {
//        JdbcTablesSupplier tableSupplier = new JdbcTablesSupplier(dataSource);
        final SqlCommandsToScripts commandsToScript = new SqlCommandsToScripts(jdbcTemplate);
        Digger.instance(commandsToScript)
            .createScript(STARTING_NODE, null, List.of(new LongID(3057665L)));
    }

    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorConsoleApplication.class, args);
    }

}
