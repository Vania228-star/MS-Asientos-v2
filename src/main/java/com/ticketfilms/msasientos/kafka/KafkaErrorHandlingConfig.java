package com.ticketfilms.msasientos.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

// Qué pasa cuando un consumidor falla al procesar un mensaje:
//
//  1) Error RECUPERABLE (ej. la BD no responde un momento):
//     se reintenta hasta 3 veces, esperando 1s, 2s y 4s entre intentos.
//  2) Error DEFINITIVO (MensajeInvalidoException):
//     no se reintenta, va directo al tópico de fallidos.
//  3) Si se acaban los reintentos: el mensaje se manda al tópico
//     "<tópico original>.DLT" para revisarlo después, y se deja en el log
//     el tópico, partición, offset y el motivo.
//  4) Solo cuando el mensaje se procesó bien (o ya quedó en el DLT)
//     se confirma, así no se pierde ni se reprocesa sin control.
//
// Spring Boot aplica este DefaultErrorHandler automáticamente a todos
// los @KafkaListener, no hay que enlazarlo a mano.
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    private static final int MAX_REINTENTOS = 3;
    private static final long ESPERA_INICIAL_MS = 1000L;
    private static final double MULTIPLICADOR = 2.0;
    private static final long ESPERA_MAXIMA_MS = 10000L;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {

        // Manda el mensaje fallido a "<topico>.DLT". Partición -1 = que Kafka elija,
        // así no importa cuántas particiones tenga el tópico original.
        DeadLetterPublishingRecoverer publicador = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (registro, ex) -> new TopicPartition(registro.topic() + KafkaTopics.SUFIJO_DLT, -1));

        // Antes de mandarlo al DLT deja en el log los datos para diagnosticar la falla
        ConsumerRecordRecoverer conRegistro = (registro, ex) -> {
            publicador.accept(registro, ex);
            log.error("Mensaje enviado a DLT — topico={} particion={} offset={} motivo={} mensaje={}",
                    registro.topic(), registro.partition(), registro.offset(),
                    ex.getMessage(), registro.value(), ex);
        };

        ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(MAX_REINTENTOS);
        espera.setInitialInterval(ESPERA_INICIAL_MS);
        espera.setMultiplier(MULTIPLICADOR);
        espera.setMaxInterval(ESPERA_MAXIMA_MS);

        DefaultErrorHandler handler = new DefaultErrorHandler(conRegistro, espera);

        // Errores definitivos: sin reintentos, directo al DLT
        handler.addNotRetryableExceptions(MensajeInvalidoException.class);

        // Un log por cada reintento, para ver el proceso en la demo
        handler.setRetryListeners((registro, ex, intento)
                -> log.warn("Reintento {} de {} — topico={} offset={} motivo={}",
                        intento, MAX_REINTENTOS, registro.topic(), registro.offset(), ex.getMessage()));

        return handler;
    }

    // Crea el tópico de fallidos de compra.fallida al arrancar si no existe
    @Bean
    public NewTopic compraFallidaDlt() {
        return TopicBuilder.name(KafkaTopics.COMPRA_FALLIDA_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }

    // Crea reserva.expirada y su DLT al arrancar si no existen
    @Bean
    public NewTopic reservaExpirada() {
        return TopicBuilder.name(KafkaTopics.RESERVA_EXPIRADA)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic reservaExpiradaDlt() {
        return TopicBuilder.name(KafkaTopics.RESERVA_EXPIRADA_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
