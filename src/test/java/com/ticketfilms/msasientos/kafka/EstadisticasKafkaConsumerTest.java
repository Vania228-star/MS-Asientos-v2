package com.ticketfilms.msasientos.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.messaging.MensajeInvalidoException;
import com.ticketfilms.msasientos.service.EstadisticasEventosService;

@ExtendWith(MockitoExtension.class)
class EstadisticasKafkaConsumerTest {

    @Mock
    private EstadisticasEventosService estadisticas;

    private EstadisticasKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new EstadisticasKafkaConsumer(estadisticas, new ObjectMapper());
    }

    @Test
    void compraFallida_cuentaPorFuncion() {
        consumer.compraFallida("{\"funcionId\":9,\"motivo\":\"La reserva expiró\"}");
        verify(estadisticas).registrarCompraFallida(9L);
    }

    @Test
    void reservaExpirada_cuentaPorFuncion() {
        consumer.reservaExpirada("{\"funcionId\":5,\"asientos\":[\"F7\"],\"emailUsuario\":\"a@b.cl\"}");
        verify(estadisticas).registrarReservaExpirada(5L);
    }

    @Test
    void jsonInvalido_esErrorDefinitivo() {
        assertThrows(MensajeInvalidoException.class, () -> consumer.compraFallida("esto no es json"));
        verifyNoInteractions(estadisticas);
    }

    @Test
    void sinFuncionId_esErrorDefinitivo() {
        assertThrows(MensajeInvalidoException.class, () -> consumer.compraFallida("{\"motivo\":\"x\"}"));
        verifyNoInteractions(estadisticas);
    }
}
