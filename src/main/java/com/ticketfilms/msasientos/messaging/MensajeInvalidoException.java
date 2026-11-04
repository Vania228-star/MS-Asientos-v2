package com.ticketfilms.msasientos.messaging;

// Error DEFINITIVO: el mensaje está mal formado o incompleto.
// Reintentar no sirve de nada, así que va directo a la DLQ.
public class MensajeInvalidoException extends RuntimeException {

    public MensajeInvalidoException(String message) {
        super(message);
    }

    public MensajeInvalidoException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
