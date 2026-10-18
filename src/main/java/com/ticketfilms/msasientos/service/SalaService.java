package com.ticketfilms.msasientos.service;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.ticketfilms.msasientos.model.Sala;
import com.ticketfilms.msasientos.repository.SalaRepository;

@Service
public class SalaService {
    
    private final SalaRepository salaRepository;

    public SalaService(SalaRepository salaRepository) {
        this.salaRepository = salaRepository;
    }

    public List<Sala> listarSalas(){
        return salaRepository.findAll();
    }

    public Optional<Sala> obtenerSalaPorId(Long id){
        return salaRepository.findById(id);
    }

    public Sala obtenerSalaPorCodigo(String sala_codigo){
        return salaRepository.findBySala_codigo(sala_codigo);
    }

    public Sala guardarSala(Sala sala){
        return salaRepository.save(sala);
    }
}
