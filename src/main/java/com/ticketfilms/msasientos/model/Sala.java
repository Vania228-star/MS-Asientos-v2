package com.ticketfilms.msasientos.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name= "sala")
@Data
public class Sala {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name= "sala_codigo", nullable = false)
    private String sala_codigo;

    @Column(name= "nombre", nullable = false)
    private String nombre;

    @Column(name= "cantidad_filas")
    private Integer cantidad_filas;

    @Column(name= "asientos_por_fila")
    private Integer asientos_por_fila;

    // NUEVOS CAMPOS para soportar múltiples verticales (Cine/Teatro vs Conciertos/Deportes)
    @Column(name= "tipo_recinto", nullable = false)
    private String tipoRecinto = "NUMERADO"; // Ej: "NUMERADO" (cine/teatro) o "GENERAL" (conciertos/estadios)[cite: 28]

    @Column(name= "aforo_maximo")
    private Integer aforoMaximo;
}
