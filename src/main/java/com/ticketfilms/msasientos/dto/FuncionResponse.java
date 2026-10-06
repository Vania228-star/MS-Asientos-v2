package com.ticketfilms.msasientos.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class FuncionResponse {
    private Long id;
    private LocalDateTime fechaHora;
    private String sala;
    private BigDecimal precioAsociado;
    private String tipoSala;
    private String subtitulada;
}