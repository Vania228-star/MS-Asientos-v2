package com.ticketfilms.msasientos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

// Grupo "ms-asientos-auditoria": deja constancia de cada compra fallida con su
// partición y offset. Como es OTRO grupo, recibe una copia completa del tópico,
// sin quitarle mensajes al grupo de analítica (fan-out). Si se agrega un grupo
// nuevo, lee desde el offset 0 (auto-offset-reset=earliest) todo lo retenido.
@Slf4j
@Component
public class AuditoriaKafkaConsumer {

    public static final String GRUPO = "ms-asientos-auditoria";

    @KafkaListener(topics = KafkaTopics.COMPRA_FALLIDA, groupId = GRUPO)
    public void auditar(ConsumerRecord<String, String> registro) {
        log.info("[auditoria] compra.fallida — particion={} offset={} clave={} valor={}",
                registro.partition(), registro.offset(), registro.key(), registro.value());
    }
}
