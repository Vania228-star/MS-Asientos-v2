package com.ticketfilms.msasientos.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Contrato compartido con ms-notificaciones.
// - asientos: etiquetas legibles ("F7", "G3") que ms-notificaciones pone en el correo.
// - asientosIds: IDs numéricos que ms-asientos usa para liberar reservas al fallar la compra.
// - emailUsuario: destino del correo (viene de Cognito, se guarda al reservar).
// - usuarioId: dueño original de la reserva (para idempotencia en liberarAsientosPorFalla).
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AsientoEventoDto {

    private Long funcionId;
    private List<String> asientos;
    private List<Long> asientosIds;
    private String usuarioId;
    private String emailUsuario;
}
