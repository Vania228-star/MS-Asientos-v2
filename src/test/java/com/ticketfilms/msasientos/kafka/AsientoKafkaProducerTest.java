package com.ticketfilms.msasientos.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class AsientoKafkaProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private AsientoKafkaProducer producer;

    @BeforeEach
    void setUp() {
        producer = new AsientoKafkaProducer(kafkaTemplate, new ObjectMapper());
    }

    @Test
    void enviarReservaExpirada_publicaElJsonEnElTopicoCorrecto() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        producer.enviarReservaExpirada(
                5L, List.of("F7", "F8"), List.of(7L, 8L), "u1", "a@b.cl");

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.RESERVA_EXPIRADA), eq("5"), json.capture());

        String cuerpo = json.getValue();
        assertTrue(cuerpo.contains("a@b.cl"));   // email de notificaciones
        assertTrue(cuerpo.contains("F7"));       // etiquetas de los asientos
        assertTrue(cuerpo.contains("F8"));
        assertTrue(cuerpo.contains("u1"));       // usuario dueño de la reserva
    }

    @Test
    void enviarReservaExpirada_usaElFuncionIdComoClaveDelMensaje() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        producer.enviarReservaExpirada(9L, List.of("A1"), List.of(1L), "u2", "c@d.cl");

        ArgumentCaptor<String> clave = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.RESERVA_EXPIRADA), clave.capture(), anyString());
        // La clave = funcionId: conserva el orden por función dentro de la partición
        assertEquals("9", clave.getValue());
    }
}