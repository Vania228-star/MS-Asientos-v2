package com.ticketfilms.msasientos.dto;

import java.util.List;
import lombok.Data;

@Data
public class ReservaRequestDto {

    private Long funcionId;
    private List<Long> asientosSolicitados;
}