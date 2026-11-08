package com.ticketfilms.msasientos.kafka;

// Único lugar donde se escriben los nombres de los tópicos.
public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String BOLETO_COMPRADO = "boleto.comprado";
    public static final String COMPRA_FALLIDA = "compra.fallida";
    public static final String RESERVA_EXPIRADA = "reserva.expirada";

    // Tópico de mensajes fallidos (DLT): llegan los que no se pudieron procesar tras los reintentos
    public static final String SUFIJO_DLT = ".DLT";
    public static final String COMPRA_FALLIDA_DLT = COMPRA_FALLIDA + SUFIJO_DLT;
    public static final String RESERVA_EXPIRADA_DLT = RESERVA_EXPIRADA + SUFIJO_DLT;
}
