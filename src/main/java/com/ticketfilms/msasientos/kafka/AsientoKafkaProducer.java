package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
@Component
@RequiredArgsConstructor 
public class AsientoKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void enviarReservaExpirada(Long funcionId,
                                      List<String> asientosEtiquetas,
                                      List<Long> asientosIds,
                                      String usuarioId,
                                      String emailUsuario) {
        AsientoEventoDto evento = new AsientoEventoDto(
                funcionId, asientosEtiquetas, asientosIds, emailUsuario, usuarioId);

        // Sin transacción (llamada simple) se publica de inmediato;
        // dentro de una transacción, recién después del commit.
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    publicar(evento);
                }
            });
        } else {
            publicar(evento);
        }
    }

    private void publicar(AsientoEventoDto evento) {
        try {
            String json = objectMapper.writeValueAsString(evento);
            String clave = String.valueOf(evento.getFuncionId());
            kafkaTemplate.send(KafkaTopics.RESERVA_EXPIRADA, clave, json)
                    .whenComplete((resultado, error) -> {
                        if (error != null) {
                            log.error("No se pudo publicar en {} — funcion={} motivo={}",
                                    KafkaTopics.RESERVA_EXPIRADA, evento.getFuncionId(),
                                    error.getMessage());
                        } else {
                            log.info("Evento publicado en {} — funcion={} usuario={}",
                                    KafkaTopics.RESERVA_EXPIRADA, evento.getFuncionId(),
                                    evento.getUsuarioId());
                        }
                    });
        } catch (Exception e) {
            log.error("Error armando el evento de reserva expirada — funcion={} motivo={}",
                    evento.getFuncionId(), e.getMessage());
        }
    }
}