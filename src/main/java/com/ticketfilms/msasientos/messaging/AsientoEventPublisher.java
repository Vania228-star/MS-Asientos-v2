package com.ticketfilms.msasientos.messaging;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.config.RabbitTopology;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Publica reserva.expirada en el exchange topic.
// Contrato: {funcionId, asientos:["F7"], asientosIds:[12], usuarioId, emailUsuario}
// ms-notificaciones usa asientos+emailUsuario; ms-asientos usa asientosIds+usuarioId.
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
        try {
            AsientoEventoDto evento = new AsientoEventoDto(
                    funcionId, asientosEtiquetas, asientosIds, usuarioId, emailUsuario);
            String json = objectMapper.writeValueAsString(evento);
            Message mensaje = MessageBuilder.withBody(json.getBytes(StandardCharsets.UTF_8))
                    .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                    .setContentEncoding("UTF-8")
                    .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                    .build();
            rabbitTemplate.send(RabbitTopology.EX_EVENTOS, RabbitTopology.RK_RESERVA_EXPIRADA, mensaje);
            log.info("Evento publicado — routingKey={} funcion={} usuario={} email={} asientos={}",
                    RabbitTopology.RK_RESERVA_EXPIRADA, funcionId, usuarioId, emailUsuario, asientosEtiquetas);
        } catch (Exception e) {
            log.error("No se pudo publicar reserva.expirada — funcion={} usuario={} motivo={}",
                    funcionId, usuarioId, e.getMessage());
        }
    }
}
