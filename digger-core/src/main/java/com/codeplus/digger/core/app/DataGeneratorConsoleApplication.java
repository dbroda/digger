package com.codeplus.digger.core.app;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.codeplus.digger.core.Digger;

@SpringBootApplication(scanBasePackages = "com.codeplus.digger")
public class DataGeneratorConsoleApplication implements CommandLineRunner {

    @Autowired
    Digger digger;

    @Override
    public void run(String... args) throws Exception {
        digger.createScript();
    }

    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorConsoleApplication.class, args);
    }

}
