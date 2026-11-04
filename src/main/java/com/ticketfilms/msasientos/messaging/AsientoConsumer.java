package com.ticketfilms.msasientos.messaging;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.ticketfilms.msasientos.config.RabbitTopology;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Cuando una compra falla en ms-boletos, libera los asientos reservados.
// El @RabbitListener solo recibe y delega el ACK/reintentos a ProcesadorAck;
// la lógica de negocio queda en consumirCompraFallida y en el servicio.
@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoConsumer {

    private final Funcion_AsientoService funcion_AsientoService;
    private final ObjectMapper objectMapper;
    private final ProcesadorAck procesadorAck;

    @RabbitListener(queues = RabbitTopology.Q_COMPRA_FALLIDA_ASIENTOS)
    public void recibirCompraFallida(Message mensaje, Channel canal) throws IOException {
        procesadorAck.procesar(mensaje, canal,
                () -> consumirCompraFallida(new String(mensaje.getBody(), StandardCharsets.UTF_8)));
    }

    public void consumirCompraFallida(String mensaje) {
        AsientoEventoDto evento = leerEvento(mensaje);
        log.info("Evento recibido [{}] funcion={} usuario={}",
                RabbitTopology.RK_COMPRA_FALLIDA, evento.getFuncionId(), evento.getUsuarioId());

        // Libera SOLO los asientos RESERVADOS por ese usuario y aguanta mensajes duplicados.
        funcion_AsientoService.liberarAsientosPorFalla(
                evento.getFuncionId(), evento.getAsientosIds(), evento.getUsuarioId());
    }

    // JSON malo o con datos faltantes = error definitivo: directo a la DLQ.
    // asientos+emailUsuario NO se validan aquí porque este consumer solo libera
    // por asientosIds; esos campos son para ms-notificaciones.
    private AsientoEventoDto leerEvento(String mensaje) {
        AsientoEventoDto evento;
        try {
            evento = objectMapper.readValue(mensaje, AsientoEventoDto.class);
        } catch (JsonProcessingException e) {
            throw new MensajeInvalidoException("El mensaje no es un JSON válido", e);
        }
        if (evento.getFuncionId() == null
                || evento.getAsientosIds() == null
                || evento.getAsientosIds().isEmpty()
                || evento.getUsuarioId() == null) {
            throw new MensajeInvalidoException("Al mensaje le faltan datos obligatorios (funcionId, asientosIds, usuarioId)");
        }
        return evento;
    }
}