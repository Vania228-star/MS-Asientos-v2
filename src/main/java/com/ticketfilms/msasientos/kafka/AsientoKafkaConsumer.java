package com.ticketfilms.msasientos.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.AsientoEventoDto;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AsientoKafkaConsumer {

    private final Funcion_AsientoService funcion_AsientoService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "boleto.comprado", groupId = "ms-asientos-group")
    public void consumirBoletoComprado(String mensaje) {
        try {
            AsientoEventoDto evento = objectMapper.readValue(mensaje, AsientoEventoDto.class);
            log.info("Evento recibido [boleto.comprado] para función: {}", evento.getFuncionId());
            funcion_AsientoService.marcarAsientosComoOcupados(evento.getFuncionId(), evento.getAsientosIds());
        } catch (Exception e) {
            log.error("Error procesando mensaje de boleto.comprado: {}", mensaje, e);
        }
    }

    @KafkaListener(topics = "compra.fallida", groupId = "ms-asientos-group")
    public void consumirCompraFallida(String mensaje) {
        try {
            AsientoEventoDto evento = objectMapper.readValue(mensaje, AsientoEventoDto.class);
            log.info("Evento recibido [compra.fallida] para función: {}", evento.getFuncionId());
            funcion_AsientoService.liberarAsientosPorFalla(evento.getFuncionId(), evento.getAsientosIds());
        } catch (Exception e) {
            log.error("Error procesando mensaje de compra.fallida: {}", mensaje, e);
        }
    }
}