package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import com.ticketfilms.msasientos.messaging.MensajeInvalidoException;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoKafkaConsumer {

    // Grupo por microservicio: cada microservicio recibe su propia copia del
    // evento (fan-out entre grupos), y dentro del grupo las 3 particiones se
    // reparten entre los consumidores (paralelismo real con 1 réplica del grupo).
    private static final String GRUPO = "ms-asientos";

    private final Funcion_AsientoService funcion_AsientoService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.COMPRA_FALLIDA, groupId = GRUPO)
    public void consumirCompraFallida(String mensaje) {
        AsientoEventoDto evento = leerEvento(mensaje);
        log.info("Evento recibido [{}] funcion={} usuario={}",
                KafkaTopics.COMPRA_FALLIDA, evento.getFuncionId(), evento.getUsuarioId());

        funcion_AsientoService.liberarAsientosPorFalla(
                evento.getFuncionId(), evento.getAsientosIds(), evento.getUsuarioId());
    }

    private AsientoEventoDto leerEvento(String mensaje) {
        AsientoEventoDto evento;
        try {
            evento = objectMapper.readValue(mensaje, AsientoEventoDto.class);
        } catch (JsonProcessingException e) {
            // JSON rojo: reintentar no sirve, va directo al DLT
            throw new MensajeInvalidoException("El mensaje no es un JSON válido", e);
        }
        if (evento.getFuncionId() == null
                || evento.getAsientosIds() == null
                || evento.getAsientosIds().isEmpty()
                || evento.getUsuarioId() == null) {
            throw new MensajeInvalidoException(
                    "Al mensaje le faltan datos obligatorios (funcionId, asientosIds, usuarioId)");
        }
        return evento;
    }
}