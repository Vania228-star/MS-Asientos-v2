package com.ticketfilms.msasientos.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.config.RabbitTopology;

@ExtendWith(MockitoExtension.class)
class AsientoEventPublisherTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    private final ObjectMapper mapper = new ObjectMapper();
    private AsientoEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new AsientoEventPublisher(rabbitTemplate, mapper);
    }

    private JsonNode cuerpoPublicado() throws Exception {
        ArgumentCaptor<Message> msg = ArgumentCaptor.forClass(Message.class);
        verify(rabbitTemplate).send(eq(RabbitTopology.EX_EVENTOS),
                eq(RabbitTopology.RK_RESERVA_EXPIRADA), msg.capture());
        return mapper.readTree(new String(msg.getValue().getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void sinTransaccion_publicaYNoIntercambiaEmailConUsuarioId() throws Exception {
        publisher.enviarReservaExpirada(5L, List.of("F7"), List.of(7L), "sub-123", "a@b.cl");

        JsonNode json = cuerpoPublicado();
        assertEquals("a@b.cl", json.get("emailUsuario").asText());
        assertEquals("sub-123", json.get("usuarioId").asText());
        assertEquals("F7", json.get("asientos").get(0).asText());
        assertEquals(7L, json.get("asientosIds").get(0).asLong());
    }

    @Test
    void conTransaccion_soloPublicaDespuesDelCommit() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            publisher.enviarReservaExpirada(5L, List.of("F7"), List.of(7L), "sub-123", "a@b.cl");

            // Aún no hay commit: no debe haber salido nada
            verify(rabbitTemplate, never()).send(eq(RabbitTopology.EX_EVENTOS),
                    eq(RabbitTopology.RK_RESERVA_EXPIRADA), org.mockito.ArgumentMatchers.any(Message.class));

            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(TransactionSynchronization::afterCommit);

            verify(rabbitTemplate).send(eq(RabbitTopology.EX_EVENTOS),
                    eq(RabbitTopology.RK_RESERVA_EXPIRADA), org.mockito.ArgumentMatchers.any(Message.class));
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
