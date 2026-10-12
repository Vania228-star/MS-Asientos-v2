package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AsientoKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private static final String TOPIC_RESERVA_EXPIRADA = "reserva.expirada";

    public void enviarReservaExpirada(Long funcionId, java.util.List<Long> asientosIds, String usuarioId) {
        try {
            AsientoEventoDto evento = new AsientoEventoDto(funcionId, asientosIds, usuarioId);
            String json = objectMapper.writeValueAsString(evento);
            kafkaTemplate.send(TOPIC_RESERVA_EXPIRADA, json);
        } catch (Exception e) {
            throw new RuntimeException("Error al serializar evento de reserva expirada", e);
        }
    }
}