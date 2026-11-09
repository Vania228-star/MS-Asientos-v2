package com.ticketfilms.msasientos.messaging;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.config.RabbitTopology;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Publica reserva.expirada en el exchange topic.
// Contrato: {funcionId, asientos:["F7"], asientosIds:[12], emailUsuario, usuarioId}
// ms-notificaciones usa asientos+emailUsuario; ms-asientos usa asientosIds+usuarioId.
// Con transacción en curso, el evento sale DESPUÉS del commit: si se revierte,
// no hay aviso falso. Si Rabbit falla, solo se registra en el log.
@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoEventPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    public void enviarReservaExpirada(Long funcionId,
                                      List<String> asientosEtiquetas,
                                      List<Long> asientosIds,
                                      String usuarioId,
                                      String emailUsuario) {
        AsientoEventoDto evento = new AsientoEventoDto();
        evento.setFuncionId(funcionId);
        evento.setAsientos(asientosEtiquetas);
        evento.setAsientosIds(asientosIds);
        evento.setUsuarioId(usuarioId);
        evento.setEmailUsuario(emailUsuario);

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
            Message mensaje = MessageBuilder.withBody(json.getBytes(StandardCharsets.UTF_8))
                    .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                    .setContentEncoding("UTF-8")
                    .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                    .build();
            rabbitTemplate.send(RabbitTopology.EX_EVENTOS, RabbitTopology.RK_RESERVA_EXPIRADA, mensaje);
            log.info("Evento publicado — routingKey={} funcion={} usuario={} email={} asientos={}",
                    RabbitTopology.RK_RESERVA_EXPIRADA, evento.getFuncionId(), evento.getUsuarioId(),
                    evento.getEmailUsuario(), evento.getAsientos());
        } catch (Exception e) {
            log.error("No se pudo publicar reserva.expirada — funcion={} usuario={} motivo={}",
                    evento.getFuncionId(), evento.getUsuarioId(), e.getMessage());
        }
    }
}
