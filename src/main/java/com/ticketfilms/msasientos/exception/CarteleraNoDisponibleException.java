package com.ticketfilms.msasientos.exception;

public class CarteleraNoDisponibleException extends RuntimeException {
    public CarteleraNoDisponibleException(Long funcionId, Throwable causa) {
        super("ms-cartelera no respondió al consultar la función " + funcionId, causa);
    }
}