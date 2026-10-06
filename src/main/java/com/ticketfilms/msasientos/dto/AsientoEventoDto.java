package com.ticketfilms.msasientos.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AsientoEventoDto {
    private Long funcionId;
    private List<Long> asientosIds;
    private String usuarioId;
}