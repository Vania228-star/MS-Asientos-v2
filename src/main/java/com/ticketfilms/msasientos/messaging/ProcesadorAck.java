package com.ticketfilms.msasientos.messaging;

import static com.ticketfilms.msasientos.config.RabbitTopology.EX_DLX;
import static com.ticketfilms.msasientos.config.RabbitTopology.dlq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import com.ticketfilms.msasientos.messaging.MensajeInvalidoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Manejo de ACK y errores de los consumidores (modo manual):
//  - éxito                      -> basicAck
//  - error RECUPERABLE          -> 3 reintentos esperando 1s, 2s y 4s
//  - error DEFINITIVO           -> directo a la DLQ (MensajeInvalidoException)
//  - reintentos agotados        -> a la DLQ
// Al ir a la DLQ se republica el mensaje en ex.tickets.dlx con headers que explican
// el fallo (motivo, clase, intentos, cola de origen, fecha) y luego se hace ACK.
// Si ni siquiera eso se puede, basicNack sin reencolar: RabbitMQ lo manda a la DLQ
// con su propio mecanismo dead-letter (x-dead-letter-exchange de la cola).
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcesadorAck {

    private static final int MAX_REINTENTOS = 3;
    private static final long ESPERA_INICIAL_MS = 1000L;

    private final RabbitTemplate rabbitTemplate;

    public void procesar(Message mensaje, Channel canal, Runnable tarea) throws IOException {
        MessageProperties props = mensaje.getMessageProperties();
        long tag = props.getDeliveryTag();
        String cola = props.getConsumerQueue();
        long espera = ESPERA_INICIAL_MS;

        for (int reintento = 0; reintento <= MAX_REINTENTOS; reintento++) {
            try {
                tarea.run();
                canal.basicAck(tag, false);
                return;
            } catch (MensajeInvalidoException e) {
                // Definitivo: reintentar no sirve de nada
                enviarADlq(mensaje, canal, cola, e, reintento + 1);
                return;
            } catch (RuntimeException e) {
                if (reintento == MAX_REINTENTOS) {
                    enviarADlq(mensaje, canal, cola, e, reintento + 1);
                    return;
                }
                log.warn("Reintento {} de {} — cola={} messageId={} motivo={}",
                        reintento + 1, MAX_REINTENTOS, cola, props.getMessageId(), e.getMessage());
                if (!dormir(espera)) {
                    canal.basicNack(tag, false, true); // apagado del servicio: que lo tome otro
                    return;
                }
                espera *= 2;
            }
        }
    }

    private void enviarADlq(Message original, Channel canal, String cola,
            Exception motivo, int intentos) throws IOException {
        long tag = original.getMessageProperties().getDeliveryTag();
        String cuerpo = new String(original.getBody(), StandardCharsets.UTF_8);
        try {
            Message conContexto = MessageBuilder.fromClonedMessage(original)
                    .setHeader("x-error-motivo", resumir(motivo))
                    .setHeader("x-error-clase", motivo.getClass().getSimpleName())
                    .setHeader("x-error-intentos", intentos)
                    .setHeader("x-error-cola-origen", cola)
                    .setHeader("x-error-fecha", LocalDateTime.now().toString())
                    .build();
            rabbitTemplate.send(EX_DLX, dlq(cola), conContexto);
            canal.basicAck(tag, false);
            log.error("Mensaje enviado a DLQ — cola={} dlq={} intentos={} clase={} motivo={} mensaje={}",
                    cola, dlq(cola), intentos, motivo.getClass().getSimpleName(), motivo.getMessage(), cuerpo);
        } catch (AmqpException ex) {
            log.error("No se pudo publicar en la DLQ, se rechaza sin reencolar — cola={} motivo={} mensaje={}",
                    cola, motivo.getMessage(), cuerpo, ex);
            canal.basicNack(tag, false, false);
        }
    }

    private String resumir(Exception e) {
        String m = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return m.length() > 500 ? m.substring(0, 500) : m;
    }

    private boolean dormir(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
