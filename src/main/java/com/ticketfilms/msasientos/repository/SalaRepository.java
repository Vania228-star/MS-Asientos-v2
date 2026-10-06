package com.ticketfilms.msasientos.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.Optional;

import com.ticketfilms.msasientos.model.Sala;

@Repository
public interface SalaRepository extends JpaRepository< Sala, Long> {
    
    @Query("SELECT s FROM Sala s WHERE s.sala_codigo = :salaCodigo")
    Sala findBySala_codigo(@Param("salaCodigo") String salaCodigo);

    Optional<Sala> findByNombre(String nombre);
}
