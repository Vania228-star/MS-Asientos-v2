package com.ticketfilms.msasientos.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.rabbitmq.client.Channel;
import com.ticketfilms.msasientos.config.RabbitTopology;

@ExtendWith(MockitoExtension.class)
class ProcesadorAckTest {

    private static final long TAG = 1L;
    private static final String COLA = RabbitTopology.Q_COMPRA_FALLIDA_ASIENTOS;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private Channel canal;

    private ProcesadorAck procesador;
    private Message mensaje;

    @BeforeEach
    void setUp() {
        procesador = new ProcesadorAck(rabbitTemplate);
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        props.setConsumerQueue(COLA);
        mensaje = new Message("{\"funcionId\":1}".getBytes(StandardCharsets.UTF_8), props);
    }

    private Message mensajeEnviadoAlDlx() {
        ArgumentCaptor<Message> enviado = ArgumentCaptor.forClass(Message.class);
        verify(rabbitTemplate).send(eq(RabbitTopology.EX_DLX), eq(RabbitTopology.dlq(COLA)), enviado.capture());
        return enviado.getValue();
    }

    @Test
    void exito_hacePAckYNoTocaLaDlq() throws Exception {
        procesador.procesar(mensaje, canal, () -> { });

        verify(canal).basicAck(TAG, false);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void errorDefinitivo_vaDirectoAlDlxSinReintentos() throws Exception {
        AtomicInteger llamadas = new AtomicInteger();

        procesador.procesar(mensaje, canal, () -> {
            llamadas.incrementAndGet();
            throw new MensajeInvalidoException("JSON inválido");
        });

        assertEquals(1, llamadas.get(), "un error definitivo no se reintenta");
        Message enDlx = mensajeEnviadoAlDlx();
        assertEquals("MensajeInvalidoException", enDlx.getMessageProperties().getHeaders().get("x-error-clase"));
        assertEquals("JSON inválido", enDlx.getMessageProperties().getHeaders().get("x-error-motivo"));
        assertEquals(COLA, enDlx.getMessageProperties().getHeaders().get("x-error-cola-origen"));
        assertEquals(1, enDlx.getMessageProperties().getHeaders().get("x-error-intentos"));
        // El cuerpo original viaja intacto
        assertEquals("{\"funcionId\":1}", new String(enDlx.getBody(), StandardCharsets.UTF_8));
        verify(canal).basicAck(TAG, false);
    }

    @Test
    void errorTransitorio_seReintentaYSiLuegoFuncionaHaceAck() throws Exception {
        AtomicInteger llamadas = new AtomicInteger();

        procesador.procesar(mensaje, canal, () -> {
            if (llamadas.incrementAndGet() == 1) {
                throw new IllegalStateException("BD no responde");
            }
        });

        assertEquals(2, llamadas.get());
        verify(canal).basicAck(TAG, false);
        verifyNoInteractions(rabbitTemplate); // se recuperó: nunca llegó a la DLQ
    }

    // Espera real de 1s + 2s + 4s = unos 7 segundos
    @Test
    void reintentosAgotados_vaAlDlxTrasCuatroIntentos() throws Exception {
        AtomicInteger llamadas = new AtomicInteger();

        procesador.procesar(mensaje, canal, () -> {
            llamadas.incrementAndGet();
            throw new IllegalStateException("BD caída");
        });

        assertEquals(4, llamadas.get(), "1 intento + 3 reintentos");
        Message enDlx = mensajeEnviadoAlDlx();
        assertEquals("IllegalStateException", enDlx.getMessageProperties().getHeaders().get("x-error-clase"));
        assertEquals(4, enDlx.getMessageProperties().getHeaders().get("x-error-intentos"));
        verify(canal).basicAck(TAG, false);
    }

    @Test
    void siFallaPublicarEnLaDlq_hacePNackSinReencolar() throws Exception {
        doThrow(new AmqpException("broker caído"))
                .when(rabbitTemplate).send(anyString(), anyString(), any(Message.class));

        procesador.procesar(mensaje, canal, () -> {
            throw new MensajeInvalidoException("datos faltantes");
        });

        // Sin reencolar: RabbitMQ lo manda a la DLQ con su dead-letter nativo
        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(TAG, false);
    }
}
