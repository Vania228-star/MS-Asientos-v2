package com.ticketfilms.msasientos.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import com.ticketfilms.msasientos.dto.FuncionResponse;

@FeignClient(name = "ms-cartelera", url = "${ms-cartelera.url}")
public interface FuncionClient {

    @GetMapping("/api/cartelera/funciones/{id}")
    FuncionResponse obtenerFuncionPorId(@PathVariable("id") Long id);
}