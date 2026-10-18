package com.ticketfilms.msasientos.dto;

import java.util.List;

public class AsientoEventoDto {
    private Long funcionId;
    private List<Long> asientosIds;
    private String usuarioId;

    public AsientoEventoDto() {
    }

    public AsientoEventoDto(Long funcionId, List<Long> asientosIds, String usuarioId) {
        this.funcionId = funcionId;
        this.asientosIds = asientosIds;
        this.usuarioId = usuarioId;
    }

    public Long getFuncionId() {
        return funcionId;
    }

    public void setFuncionId(Long funcionId) {
        this.funcionId = funcionId;
    }

    public List<Long> getAsientosIds() {
        return asientosIds;
    }

    public void setAsientosIds(List<Long> asientosIds) {
        this.asientosIds = asientosIds;
    }

    public String getUsuarioId() {
        return usuarioId;
    }

    public void setUsuarioId(String usuarioId) {
        this.usuarioId = usuarioId;
    }
}