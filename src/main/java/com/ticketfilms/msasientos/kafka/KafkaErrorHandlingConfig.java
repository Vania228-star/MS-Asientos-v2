package com.ticketfilms.msasientos.kafka;

import com.ticketfilms.msasientos.messaging.MensajeInvalidoException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    private static final String RETENCION_NEGOCIO_MS = "604800000";
    private static final String RETENCION_DLT_MS = "259200000";

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(template,
                        (record, ex) -> new TopicPartition(
                                record.topic() + KafkaTopics.SUFIJO_DLT, record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(10);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2);
        backOff.setMaxInterval(30_000);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // El error definitivo NO se reintenta: cae derecho al DLT
        handler.addNotRetryableExceptions(MensajeInvalidoException.class);
        handler.setRetryListeners((record, ex, intento) ->
                log.warn("Reintento {} de {} — clave={} motivo={}",
                        intento, record.topic(), record.key(), ex.getMessage()));
        return handler;
    }

    // Tópico compra.fallida: acá ms-asientos escucha los fallos de compra.
    // Si ms-boletos ya lo creó con 1×1, bórralo en kafka-ui y deja que este
    // bean lo cree con 3×3 (Kafka no modifica tópicos existentes).
    @Bean
    public NewTopic compraFallida() {
        return TopicBuilder.name(KafkaTopics.COMPRA_FALLIDA)
                .partitions(3).replicas(3)
                .config("retention.ms", RETENCION_NEGOCIO_MS)
                .build();
    }

    // DLT de compra.fallida: fallas definitivas tras los reintentos.
    @Bean
    public NewTopic compraFallidaDlt() {
        return TopicBuilder.name(KafkaTopics.COMPRA_FALLIDA_DLT)
                .partitions(3).replicas(3)
                .config("retention.ms", RETENCION_DLT_MS)
                .build();
    }

    // Tópico reserva.expirada: ms-asientos avisa que reservas vencieron.
    @Bean
    public NewTopic reservaExpirada() {
        return TopicBuilder.name(KafkaTopics.RESERVA_EXPIRADA)
                .partitions(3).replicas(3)
                .config("retention.ms", RETENCION_NEGOCIO_MS)
                .build();
    }

    // DLT de reserva.expirada.
    @Bean
    public NewTopic reservaExpiradaDlt() {
        return TopicBuilder.name(KafkaTopics.RESERVA_EXPIRADA_DLT)
                .partitions(3).replicas(3)
                .config("retention.ms", RETENCION_DLT_MS)
                .build();
    }
}