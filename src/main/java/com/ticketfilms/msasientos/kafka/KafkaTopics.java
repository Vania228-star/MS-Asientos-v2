package com.ticketfilms.msasientos.kafka;

// unico lugar donde se escriben los nombres de los tópicos.
// Productores, consumidores y la configuración de errores usan estas constantes,
// así un cambio de nombre se hace una sola vez.
public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String BOLETO_COMPRADO = "boleto.comprado";
    public static final String COMPRA_FALLIDA = "compra.fallida";
    public static final String RESERVA_EXPIRADA = "reserva.expirada";

    // tópico de mensajes fallidos (DLT): aquí llegan los mensajes que
    // no se pudieron procesar después de los reintentos
    public static final String SUFIJO_DLT = ".DLT";
    public static final String COMPRA_FALLIDA_DLT = COMPRA_FALLIDA + SUFIJO_DLT;
    public static final String RESERVA_EXPIRADA_DLT = RESERVA_EXPIRADA + SUFIJO_DLT;
}
