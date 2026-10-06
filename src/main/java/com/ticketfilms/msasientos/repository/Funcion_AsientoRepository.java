package com.ticketfilms.msasientos.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.ticketfilms.msasientos.model.Funcion_Asiento;

import jakarta.persistence.LockModeType;

@Repository
public interface Funcion_AsientoRepository extends JpaRepository<Funcion_Asiento, Long> {

    @Query("SELECT f FROM Funcion_Asiento f WHERE f.funcion_id = :funcionId")
    List<Funcion_Asiento> findByFuncion_id(@Param("funcionId") Long funcionId);

    @Query("SELECT f FROM Funcion_Asiento f WHERE f.funcion_id = :funcionId AND f.asiento_id = :asientoId")
    Optional<Funcion_Asiento> findByFuncion_idAndAsiento_id(@Param("funcionId") Long funcionId, @Param("asientoId") Long asientoId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT f FROM Funcion_Asiento f WHERE f.funcion_id = :funcionId AND f.asiento_id = :asientoId")
    Optional<Funcion_Asiento> findByFuncion_idAndAsiento_idForUpdate(@Param("funcionId") Long funcionId, @Param("asientoId") Long asientoId);

    @Query("SELECT f FROM Funcion_Asiento f WHERE f.usuario_id = :usuarioId")
    List<Funcion_Asiento> findByUsuario_id(@Param("usuarioId") String usuarioId);
}