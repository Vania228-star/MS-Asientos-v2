package com.ticketfilms.msasientos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class MsasientosApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsasientosApplication.class, args);
    }

}