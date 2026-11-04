package com.ticketfilms.msasientos.config;

import static com.ticketfilms.msasientos.config.RabbitTopology.*;

import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Topología de RabbitMQ en un solo lugar. Solo declara infraestructura:
// no tiene lógica de negocio. RabbitAdmin la crea al conectarse.
//
//  ex.tickets.eventos (topic)  --boleto.comprado-->  q.boleto.comprado.notificaciones
//                              --compra.fallida --->  q.compra.fallida.notificaciones
//                              --compra.fallida --->  q.compra.fallida.asientos
//                              --reserva.expirada->  q.reserva.expirada.notificaciones
//
//  Cada cola tiene x-dead-letter-exchange = ex.tickets.dlx (direct) y su DLQ "<cola>.dlq".
@Configuration
public class RabbitTopologyConfig {

    @Bean
    public TopicExchange exchangeEventos() {
        return new TopicExchange(EX_EVENTOS, true, false);
    }

    @Bean
    public DirectExchange exchangeDlx() {
        return new DirectExchange(EX_DLX, true, false);
    }

    @Bean
    public Declarables colasYBindings(TopicExchange exchangeEventos, DirectExchange exchangeDlx) {
        List<Declarable> d = new ArrayList<>();
        agregar(d, exchangeEventos, exchangeDlx, Q_BOLETO_COMPRADO, RK_BOLETO_COMPRADO);
        agregar(d, exchangeEventos, exchangeDlx, Q_COMPRA_FALLIDA_NOTIFICACIONES, RK_COMPRA_FALLIDA);
        agregar(d, exchangeEventos, exchangeDlx, Q_COMPRA_FALLIDA_ASIENTOS, RK_COMPRA_FALLIDA);
        agregar(d, exchangeEventos, exchangeDlx, Q_RESERVA_EXPIRADA, RK_RESERVA_EXPIRADA);
        return new Declarables(d);
    }

    // Cola + su DLQ + binding al exchange de eventos + binding de la DLQ al DLX
    private void agregar(List<Declarable> d, TopicExchange eventos, DirectExchange dlx,
            String nombreCola, String routingKey) {
        Queue cola = QueueBuilder.durable(nombreCola)
                .deadLetterExchange(EX_DLX)
                .deadLetterRoutingKey(dlq(nombreCola))
                .build();
        Queue colaDlq = QueueBuilder.durable(dlq(nombreCola)).build();

        Binding bindingCola = BindingBuilder.bind(cola).to(eventos).with(routingKey);
        Binding bindingDlq = BindingBuilder.bind(colaDlq).to(dlx).with(dlq(nombreCola));

        d.add(cola);
        d.add(colaDlq);
        d.add(bindingCola);
        d.add(bindingDlq);
    }
}
