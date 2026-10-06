package com.ticketfilms.msasientos.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(FuncionNoEncontradaException.class)
    public ResponseEntity<Map<String, String>> manejarFuncionNoEncontrada(FuncionNoEncontradaException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", ex.getMessage()));
    }

    @ExceptionHandler(CarteleraNoDisponibleException.class)
    public ResponseEntity<Map<String, String>> manejarCarteleraNoDisponible(CarteleraNoDisponibleException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", ex.getMessage()));
    }
}