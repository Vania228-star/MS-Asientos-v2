package com.ticketfilms.msasientos.kafka;

import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AsientoKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC_RESERVA_EXPIRADA = "reserva.expirada";

    public void enviarReservaExpirada(Long funcionId, java.util.List<Long> asientosIds, String usuarioId) {
        AsientoEventoDto evento = new AsientoEventoDto(funcionId, asientosIds, usuarioId);
        kafkaTemplate.send(TOPIC_RESERVA_EXPIRADA, evento);
    }
}