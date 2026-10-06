package com.ticketfilms.msasientos.kafka;

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

    @KafkaListener(topics = "boleto.comprado", groupId = "ms-asientos-group")
    public void consumirBoletoComprado(AsientoEventoDto evento) {
        log.info("Evento recibido [boleto.comprado] para función: {}", evento.getFuncionId());
        funcion_AsientoService.marcarAsientosComoOcupados(evento.getFuncionId(), evento.getAsientosIds());
    }

    @KafkaListener(topics = "compra.fallida", groupId = "ms-asientos-group")
    public void consumirCompraFallida(AsientoEventoDto evento) {
        log.info("Evento recibido [compra.fallida] para función: {}", evento.getFuncionId());
        funcion_AsientoService.liberarAsientosPorFalla(evento.getFuncionId(), evento.getAsientosIds());
    }
}