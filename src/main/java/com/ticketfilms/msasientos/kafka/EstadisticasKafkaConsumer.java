package com.ticketfilms.msasientos.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.messaging.MensajeInvalidoException;
import com.ticketfilms.msasientos.service.EstadisticasEventosService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Grupo "ms-asientos-analitica": cuenta eventos por función.
// concurrency=3: tres hilos, y Kafka reparte las 3 particiones entre ellos
// (paralelismo real). Los eventos de una misma función llevan la misma clave,
// así que caen en la misma partición y se procesan en orden.
// Si el JSON es inválido, el error es definitivo: va directo al DLT.
@Slf4j
@Component
@RequiredArgsConstructor
public class EstadisticasKafkaConsumer {

    public static final String GRUPO = "ms-asientos-analitica";

    private final EstadisticasEventosService estadisticas;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.COMPRA_FALLIDA, groupId = GRUPO, concurrency = "3")
    public void compraFallida(String mensaje) {
        Long funcionId = leerFuncionId(mensaje);
        estadisticas.registrarCompraFallida(funcionId);
        log.info("[analitica] compra.fallida contada — funcion={}", funcionId);
    }

    @KafkaListener(topics = KafkaTopics.RESERVA_EXPIRADA, groupId = GRUPO, concurrency = "3")
    public void reservaExpirada(String mensaje) {
        Long funcionId = leerFuncionId(mensaje);
        estadisticas.registrarReservaExpirada(funcionId);
        log.info("[analitica] reserva.expirada contada — funcion={}", funcionId);
    }

    Long leerFuncionId(String mensaje) {
        JsonNode nodo;
        try {
            nodo = objectMapper.readTree(mensaje);
        } catch (JsonProcessingException e) {
            throw new MensajeInvalidoException("El mensaje no es un JSON válido", e);
        }
        if (nodo == null || !nodo.hasNonNull("funcionId")) {
            throw new MensajeInvalidoException("Al mensaje le falta funcionId");
        }
        return nodo.get("funcionId").asLong();
    }
}
