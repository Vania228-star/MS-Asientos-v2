package com.ticketfilms.msasientos.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.repository.AsientoRepository;

@Service
public class AsientoService {

    private final AsientoRepository asientoRepository;

    public AsientoService(AsientoRepository asientoRepository) {
        this.asientoRepository = asientoRepository;
    }

    public List<Asiento> obtenerAsientosPorSala(Long sala_id){
        return asientoRepository.findBySalaId(sala_id);
    }
}
