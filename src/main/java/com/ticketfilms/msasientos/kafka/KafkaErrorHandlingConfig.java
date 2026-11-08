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

@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    private static final int PARTICIONES = 3;
    private static final int REPLICAS = 3;
    private static final String MIN_ISR = "2";
    private static final String RETENCION_NEGOCIO_MS = "604800000"; // 7 días
    private static final String RETENCION_DLT_MS = "259200000";     // 3 días

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer
                = new DeadLetterPublishingRecoverer(template,
                        (record, ex) -> new TopicPartition(
                                record.topic() + KafkaTopics.SUFIJO_DLT, record.partition()));

        // Configuración del backoff exponencial
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(30_000);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
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
