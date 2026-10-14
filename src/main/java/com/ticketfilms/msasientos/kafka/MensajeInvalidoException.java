package com.ticketfilms.msasientos.kafka;

// error DEFINITIVO: el mensaje esta mal formado o incompleto.
// Reintentar no sitve de nada, asi que va directo al topico de fallidos DLT.
public class MensajeInvalidoException extends RuntimeException {

    public MensajeInvalidoException(String message) {
        super(message);
    }

    public MensajeInvalidoException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
