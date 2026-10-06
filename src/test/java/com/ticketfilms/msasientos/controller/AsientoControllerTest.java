package com.ticketfilms.msasientos.controller;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketfilms.msasientos.dto.ReservaRequestDto;
import com.ticketfilms.msasientos.model.Asiento;
import com.ticketfilms.msasientos.model.Funcion_Asiento;
import com.ticketfilms.msasientos.model.Sala;
import com.ticketfilms.msasientos.service.AsientoService;
import com.ticketfilms.msasientos.service.Funcion_AsientoService;
import com.ticketfilms.msasientos.service.SalaService;

@WebMvcTest(AsientoController.class)
public class AsientoControllerTest {
    
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private AsientoService asientoService;

    @MockBean
    private Funcion_AsientoService funcion_AsientoService;

    @MockBean
    private SalaService salaService;

    @Test
    public void testListarSalaEndpoint() throws Exception {
        Sala sala = new Sala();
        sala.setId(1L);
        sala.setNombre("Sala VIP");

        when(salaService.listarSalas()).thenReturn(List.of(sala));

        mockMvc.perform(get("/api/asientos/sala"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testObtenerAsientosPorSalaEndpoint() throws Exception{
        Asiento asiento = new Asiento();
        asiento.setId(1L);
        asiento.setSala_id(1L);

        when(asientoService.obtenerAsientosPorSala(1L)).thenReturn(List.of(asiento));

        mockMvc.perform(get("/api/asientos/sala/1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testObtenerMapaFuncionEndpoint() throws Exception{
        Funcion_Asiento fa = new Funcion_Asiento();
        fa.setId(1L);
        fa.setFuncion_id(1L);

        when(funcion_AsientoService.obtenerMapaFuncion_Asientos(1L)).thenReturn(List.of(fa));

        mockMvc.perform(get("/api/asientos/mapa/1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test

    public void testReservaAsientosEndpoint() throws Exception{
        ReservaRequestDto request = new ReservaRequestDto();
        request.setFuncionId(1L);
        request.setAsientosSolicitados(List.of(1L, 2L));

        when(funcion_AsientoService.reservarAsientos(anyString(), anyLong(), anyList()))
                .thenReturn(true);

        mockMvc.perform(post("/api/asientos/reserva")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(content().string("Asientos reservados temporalmente con éxito"));
    }

    @Test
    public void testListarReservaEndpoint() throws Exception{
        mockMvc.perform(delete("/api/asientos/reserva/1"))
        .andExpect(status().isNoContent());
    }
}