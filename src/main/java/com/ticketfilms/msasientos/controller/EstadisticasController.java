package com.ticketfilms.msasientos.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ticketfilms.msasientos.service.EstadisticasEventosService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/asientos/estadisticas")
@RequiredArgsConstructor
public class EstadisticasController {

    private final EstadisticasEventosService estadisticas;

    // Resultado del consumo en Kafka: sirve para mostrar que el sistema reacciona a eventos
    @GetMapping("/eventos")
    public ResponseEntity<Map<String, Object>> eventos() {
        return ResponseEntity.ok(estadisticas.resumen());
    }
}
