package com.ticketfilms.msasientos.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name= "asiento")
@Data
public class Asiento {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name= "sala_id", nullable = false)
    private Long sala_id;

    @Column(name= "fila", nullable = false)
    private String fila;

    @Column(name= "numero", nullable = false)
    private Integer numero;


    @Column(name= "categoria", nullable = false)
    private String categoria;
}
