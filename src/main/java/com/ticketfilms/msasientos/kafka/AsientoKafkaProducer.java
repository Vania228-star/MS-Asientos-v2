package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import java.util.List;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AsientoKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public AsientoKafkaProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void enviarReservaExpirada(Long funcionId, List<Long> asientosIds, String usuarioId) {
        try {
            AsientoEventoDto evento = new AsientoEventoDto(funcionId, asientosIds, usuarioId);
            String json = objectMapper.writeValueAsString(evento);
            kafkaTemplate.send(KafkaTopics.RESERVA_EXPIRADA, json);
        } catch (Exception e) {
            throw new RuntimeException("Error al serializar evento de reserva expirada", e);
        }
    }
}
