package com.codeplus.digger.examples.app;

import com.codeplus.digger.core.Digger;
import com.codeplus.digger.jdbc.JdbcTablesSupplier;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DataGeneratorConsoleApplication implements CommandLineRunner {

    @Autowired
    private DataSource dataSource;

    @Override
    public void run(String... args) throws Exception {
        Digger.instance().createScript(new JdbcTablesSupplier(dataSource));
    }

    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorConsoleApplication.class, args);
    }

}
