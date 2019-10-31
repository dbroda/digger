package com.codeplus.digger.core;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration()
class Conf {


    @Bean
    Digger datagenFacade(@Autowired DataSource dataSource) {
        return new Digger(new DatabaseSchemaLoader(dataSource),
            new GraphFromTablesBuilder(), new GraphToCommandsService());
    }

}
