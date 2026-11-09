package com.ticketfilms.msasientos.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Service;

// Contadores en memoria de los eventos leídos desde Kafka (por función).
// Solo cuenta: la lógica de negocio vive aquí y el consumidor solo recibe.
// Límite: están en memoria, se pierden al reiniciar (al reiniciar, el grupo
// vuelve a leer desde su último offset confirmado, no desde cero).
@Service
public class EstadisticasEventosService {

    private final Map<Long, AtomicLong> comprasFallidas = new ConcurrentHashMap<>();
    private final Map<Long, AtomicLong> reservasExpiradas = new ConcurrentHashMap<>();

    public void registrarCompraFallida(Long funcionId) {
        comprasFallidas.computeIfAbsent(funcionId, k -> new AtomicLong()).incrementAndGet();
    }

    public void registrarReservaExpirada(Long funcionId) {
        reservasExpiradas.computeIfAbsent(funcionId, k -> new AtomicLong()).incrementAndGet();
    }

    public Map<String, Object> resumen() {
        return Map.of(
                "comprasFallidasPorFuncion", copiar(comprasFallidas),
                "reservasExpiradasPorFuncion", copiar(reservasExpiradas));
    }

    private Map<Long, Long> copiar(Map<Long, AtomicLong> origen) {
        Map<Long, Long> copia = new ConcurrentHashMap<>();
        origen.forEach((k, v) -> copia.put(k, v.get()));
        return copia;
    }
}
