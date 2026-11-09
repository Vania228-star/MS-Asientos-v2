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
import org.springframework.util.backoff.ExponentialBackOff;

// Manejo de errores de Kafka + definición de tópicos (3 particiones x 3 réplicas).
// Versión combinada: estructura de Oriana (constantes + min.insync.replicas=2)
// + tope de reintentos para que los errores transitorios terminen en el DLT.
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    private static final int PARTICIONES = 3;
    private static final int REPLICAS = 3;
    private static final String MIN_ISR = "2";
    private static final String RETENCION_NEGOCIO_MS = "604800000"; // 7 días
    private static final String RETENCION_DLT_MS = "259200000";     // 3 días

    // Reintentos exponenciales + DLT:
    //  - Transitorio (broker caído): reintenta 500ms -> 1s -> 2s ... hasta 30s.
    //  - Tope de 1 min (maxElapsedTime): si el error persiste, el mensaje va
    //    al DLT en vez de reintentar para siempre (no se pierde, se audita).
    //  - Mensaje inválido (JSON rojo / campos faltantes): SIN reintento,
    //    va directo al DLT porque reintentar no sirve de nada.
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer
                = new DeadLetterPublishingRecoverer(template,
                        (record, ex) -> new TopicPartition(
                                record.topic() + KafkaTopics.SUFIJO_DLT, record.partition()));

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(30_000);
        backOff.setMaxElapsedTime(60_000); // 1 min reintentando y se rinde -> DLT

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // El error definitivo NO se reintenta: cae derecho al DLT
        handler.addNotRetryableExceptions(MensajeInvalidoException.class);

        handler.setRetryListeners((record, ex, intento)
                -> log.warn("Reintento {} — topico={} clave={} motivo={}",
                        intento, record.topic(), record.key(), ex.getMessage()));
        return handler;
    }

    private NewTopic topico(String nombre, String retencionMs) {
        return TopicBuilder.name(nombre)
                .partitions(PARTICIONES).replicas(REPLICAS)
                .config("min.insync.replicas", MIN_ISR)
                .config("retention.ms", retencionMs)
                .build();
    }

    @Bean
    public NewTopic compraFallida() {
        return topico(KafkaTopics.COMPRA_FALLIDA, RETENCION_NEGOCIO_MS);
    }

    @Bean
    public NewTopic compraFallidaDlt() {
        return topico(KafkaTopics.COMPRA_FALLIDA_DLT, RETENCION_DLT_MS);
    }

    @Bean
    public NewTopic reservaExpirada() {
        return topico(KafkaTopics.RESERVA_EXPIRADA, RETENCION_NEGOCIO_MS);
    }

    @Bean
    public NewTopic reservaExpiradaDlt() {
        return topico(KafkaTopics.RESERVA_EXPIRADA_DLT, RETENCION_DLT_MS);
    }
}
