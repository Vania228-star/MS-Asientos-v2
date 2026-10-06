package com.ticketfilms.msasientos.exception;

public class FuncionNoEncontradaException extends RuntimeException {
    public FuncionNoEncontradaException(Long funcionId) {
        super("No existe la función con id " + funcionId + " en ms-cartelera");
    }
}