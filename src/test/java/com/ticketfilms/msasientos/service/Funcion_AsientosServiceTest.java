package com.ticketfilms.msasientos.service;

import com.ticketfilms.msasientos.client.FuncionClient;
import com.ticketfilms.msasientos.dto.FuncionResponse;
import com.ticketfilms.msasientos.exception.FuncionNoEncontradaException;
import com.ticketfilms.msasientos.kafka.AsientoKafkaProducer;
import com.ticketfilms.msasientos.messaging.AsientoEventPublisher;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.model.Sala;
import com.ticketfilms.msasientos.repository.AsientoRepository;
import com.ticketfilms.msasientos.repository.Funcion_AsientoRepository;
import com.ticketfilms.msasientos.repository.SalaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import com.ticketfilms.msasientos.kafka.AsientoKafkaProducer;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class Funcion_AsientosServiceTest {

    @Mock
    private Funcion_AsientoRepository funcion_AsientoRepository;
    @Mock
    private AsientoRepository asientoRepository;
    @Mock
    private SalaRepository salaRepository;
    @Mock
    private FuncionClient funcionClient;
    @Mock
    private AsientoEventPublisher asientoEventPublisher;   // <-- corrected type
    @Mock
    private AsientoKafkaProducer asientoKafkaProducer;

    @InjectMocks
    private Funcion_AsientoService funcionAsientoService;

    // ---------- reservarAsientos ----------
    @Test
    void reservarAsientos_devuelveFalse_cuandoListaVacia() {
        boolean ok = funcionAsientoService.reservarAsientos("user-1", 1L, List.of());
        assertThat(ok).isFalse();
        verifyNoInteractions(funcion_AsientoRepository);
    }

    @Test
    void reservarAsientos_devuelveFalse_cuandoSuperaMaximo() {
        List<Long> ids = List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        boolean ok = funcionAsientoService.reservarAsientos("user-1", 1L, ids);
        assertThat(ok).isFalse();
        verifyNoInteractions(funcion_AsientoRepository);
    }

    @Test
    void reservarAsientos_devuelveFalse_cuandoAlgunAsientoNoExiste() {
        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(1L, 1L))
                .thenReturn(Optional.empty());

        boolean ok = funcionAsientoService.reservarAsientos("user-1", 1L, List.of(1L));

        assertThat(ok).isFalse();
        verify(funcion_AsientoRepository, never()).saveAll(any());
    }

    @Test
    void reservarAsientos_devuelveFalse_cuandoAsientoOcupadoPorOtro() {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setEstado("OCUPADO");
        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(1L, 1L))
                .thenReturn(Optional.of(fa));

        boolean ok = funcionAsientoService.reservarAsientos("user-1", 1L, List.of(1L));

        assertThat(ok).isFalse();
    }

    @Test
    void reservarAsientos_ok_cuandoDisponibles() {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setEstado("DISPONIBLE");
        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(1L, 1L))
                .thenReturn(Optional.of(fa));

        boolean ok = funcionAsientoService.reservarAsientos(
                "user-1", "user1@mail.com", 1L, List.of(1L));

        assertThat(ok).isTrue();
        assertThat(fa.getEstado()).isEqualTo("RESERVADO");
        assertThat(fa.getUsuario_id()).isEqualTo("user-1");
        assertThat(fa.getEmail_usuario()).isEqualTo("user1@mail.com");
        assertThat(fa.getReservado_hasta()).isAfter(LocalDateTime.now());
        verify(funcion_AsientoRepository).saveAll(any());
        // No hay reservas vencidas ajenas que avisar
        verifyNoInteractions(asientoEventPublisher);
    }

    @Test
    void reservarAsientos_publicaExpiradas_cuandoHabiaReservaVencidaAjena() {
        // 1. La reserva vencida debe tener funcion_id y asiento_id seteados
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setFuncion_id(1L);
        fa.setAsiento_id(10L);
        fa.setEstado("RESERVADO");
        fa.setUsuario_id("otro-user");
        fa.setEmail_usuario("otro@mail.com");
        fa.setReservado_hasta(LocalDateTime.now().minusMinutes(1));
        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(1L, 1L))
                .thenReturn(Optional.of(fa));

        // 2. etiquetas() consulta asientoRepository.findById → mockearlo
        Asiento asiento = new Asiento();
        asiento.setId(10L);
        asiento.setFila("F");
        asiento.setNumero(7);
        when(asientoRepository.findAllById(List.of(10L))).thenReturn(List.of(asiento));

        boolean ok = funcionAsientoService.reservarAsientos("user-1", 1L, List.of(1L));

        assertThat(ok).isTrue();
        verify(asientoEventPublisher).enviarReservaExpirada(
                eq(1L), // funcion_id ahora sí es 1L
                eq(List.of("F7")), // etiqueta legible
                eq(List.of(10L)), // asiento_id ahora sí es 10L
                eq("otro-user"),
                eq("otro@mail.com"));
    }

    // ---------- confirmarAsientos ----------
    @Test
    void confirmarAsientos_devuelveReservaExpirada_cuandoVencioPropiaReserva() {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setEstado("RESERVADO");
        fa.setUsuario_id("user-1");
        fa.setReservado_hasta(LocalDateTime.now().minusSeconds(10));
        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(1L, 1L))
                .thenReturn(Optional.of(fa));

        String result = funcionAsientoService.confirmarAsientos("user-1", 1L, List.of(1L));

        assertThat(result).isEqualTo("RESERVA_EXPIRADA");
        assertThat(fa.getEstado()).isEqualTo("DISPONIBLE");
        // El dueño es el mismo usuario que confirma → no se publica evento
        verifyNoInteractions(asientoEventPublisher);
    }

    @Test
    void confirmarAsientos_ok() {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setEstado("RESERVADO");
        fa.setUsuario_id("user-1");
        fa.setReservado_hasta(LocalDateTime.now().plusMinutes(3));
        when(funcion_AsientoRepository.findByFuncion_idAndAsiento_idForUpdate(1L, 1L))
                .thenReturn(Optional.of(fa));

        String result = funcionAsientoService.confirmarAsientos("user-1", 1L, List.of(1L));

        assertThat(result).isEqualTo("OK");
        assertThat(fa.getEstado()).isEqualTo("OCUPADO");
    }

    // ---------- obtenerMapaFuncion_Asientos ----------
    @Test
    void obtenerMapa_limpiaExpiradasYPublicaEvento() {
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setFuncion_id(1L);
        fa.setAsiento_id(10L);
        fa.setEstado("RESERVADO");
        fa.setUsuario_id("user-1");
        fa.setEmail_usuario("user1@mail.com");
        fa.setReservado_hasta(LocalDateTime.now().minusMinutes(1));

        when(funcion_AsientoRepository.findByFuncion_id(1L)).thenReturn(List.of(fa));

        List<Funcion_Asiento> result
                = funcionAsientoService.obtenerMapaFuncion_Asientos(1L);

        assertThat(result).hasSize(1);
        assertThat(fa.getEstado()).isEqualTo("DISPONIBLE");
        verify(asientoEventPublisher).enviarReservaExpirada(
                eq(1L), any(), eq(List.of(10L)), eq("user-1"), eq("user1@mail.com"));
    }
}
